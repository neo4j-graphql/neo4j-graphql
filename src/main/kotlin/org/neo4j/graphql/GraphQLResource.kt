package org.neo4j.graphql

import com.fasterxml.jackson.databind.ObjectMapper
import graphql.GraphQL
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.logging.Log
import org.neo4j.logging.LogProvider
import java.io.PrintWriter
import java.io.StringWriter
import java.lang.IllegalStateException
import javax.ws.rs.*
import javax.ws.rs.core.Context
import javax.ws.rs.core.HttpHeaders
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import kotlin.streams.toList
/**
 * @author mh
 * @since 30.10.16
 */
@Path("/")
class GraphQLResource(@Context val provider: LogProvider, @Context val dbms: DatabaseManagementService) {
    val log: Log
    init {
        log = provider.getLog(GraphQLResource::class.java)
    }

    companion object {
        val OBJECT_MAPPER: ObjectMapper = ObjectMapper()
    }

    @Path("")
    @OPTIONS
    fun options(@Context headers: HttpHeaders) = Response.ok().build()

    @Path("/{db}/")
    @GET
    fun get(@PathParam("db") dbName:String, @QueryParam("query") query: String?, @QueryParam("variables") variableParam: String?): Response {
        if (query == null) return Response.noContent().build()
        return executeQuery(dbName, hashMapOf("query" to query, "variables" to (variableParam ?: emptyMap<String,Any>())))
    }

    @Path("/{db}/")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    fun executeOperation(@PathParam("db") dbName: String, body: String): Response {
        return executeQuery(dbName, parseMap(body))
    }

    @Path("/{db}/idl")
    @POST
    fun storeIdl(@PathParam("db") dbName: String, schema: String): Response {
        try {
            val text = if (schema.trim().startsWith('{')) {
                parseMap(schema).get("query")?.toString() ?: throw IllegalArgumentException("Can't read schema as JSON despite starting with '{'")
            } else {
                if (schema.trim().let { it.startsWith('"') && it.endsWith('"') }) schema.trim('"', ' ', '\t', '\n') else schema
            }
            SchemaBuilder.buildSchema(text, SchemaConfig())
            SchemaStorage.updateSchema(dbms,dbName,text)
            // TODO parse test
            // OBJECT_MAPPER.writeValueAsString(schema)
            return Response.ok().entity(schema).build()
        } catch(e: Exception) {
            return Response.serverError().entity(GraphQLResource.OBJECT_MAPPER.writeValueAsString(mapOf("error" to e.message,"trace" to e.stackTraceAsString()))).build()
        }
    }

    fun Throwable.stackTraceAsString(): String {
        val sw = StringWriter()
        this.printStackTrace(PrintWriter(sw))
        return sw.toString()
    }

    @Path("/{db}/idl")
    @DELETE
    fun deleteIdl(@PathParam("db") dbName: String): Response {
        SchemaStorage.deleteSchema(dbms,dbName)
        return Response.ok().build() // todo JSON
    }

    @Path("/{db}/idl")
    @GET
    fun getIdl(@PathParam("db") dbName: String): Response {
        val props = SchemaStorage.schemaProperties(dbms, dbName)
        return props.get("schema")?.let {schema ->
            // val printed = SchemaPrinter().print(schema)
            return Response.ok().entity(schema).build() // todo JSON
        } ?: Response.noContent().build()
    }

    private fun executeQuery(dbName: String, params: Map<String, Any>): Response {
        val query = params["query"] as String
        val variables = getVariables(params)
        if (log.isDebugEnabled()) log.debug("Executing {} with {}", query, variables)
        dbms.database(dbName).beginTx().use { tx ->
            val result: Map<String,Any> = try {
                val schemaIdl = SchemaStorage.schemaProperties(dbms, dbName).get("schema") as String?
                        ?: throw IllegalStateException("No Schema available for " + dbName)
                val schemaConfig = SchemaConfig()
                val schema = SchemaBuilder.buildSchema(schemaIdl, schemaConfig)
                if (query.contains("__schema")) {
                    val res = GraphQL.newGraphQL(schema).build().execute(query)
                    mapOf<String,Any>("data" to res.getData<Any>(), "errors" to res.errors, "extensions" to res.extensions)
                } else {
                    val queries = Translator(schema).translate(query, variables, QueryContext())
                    // todo return query-key/alias
                    val results = queries.map { cypher -> tx.execute(cypher.query, cypher.params).let { r ->
                        val isList = cypher.type?.isList() ?: false
                        val col = r.columns().first()
                        mapOf(col to r.columnAs<Any>(col).asSequence().let { if (isList) it.toList() else it.firstOrNull() })
                    } }
                    tx.commit()
                    mapOf("data" to results)
                }
            } catch (e: Exception) {
                log.warn("Errors: {}", e)
                tx.rollback()
                mapOf("errors" to e.message as Any)
            }
// TODO Errors, metadata
/*
            val ctx = GraphQLContext(db, log, variables)
            val graphQL = GraphSchema.getGraphQL(db)
            val execution = ExecutionInput.Builder()
                    .query(query).variables(variables).context(ctx).root(ctx) // todo proper mutation root
            params.get("operationName")?.let { execution.operationName(it.toString()) }
            val executionResult = graphQL.execute(execution.build())

            if (executionResult.errors.isNotEmpty()) {
                log.warn("Errors: {}", executionResult.errors)
                result.put("errors", executionResult.errors)
                tx.failure()
            } else {
                tx.success()
            }
*/
            return Response.ok().entity(formatMap(result)).build()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun getVariables(requestBody: Map<String, Any?>): Map<String, Any> {
        val varParam = requestBody["variables"]
        return when (varParam) {
            is String -> parseMap(varParam)
            is Map<*, *> -> varParam as Map<String, Any>
            else -> emptyMap()
        }
    }

    private fun formatMap(result: Any) = OBJECT_MAPPER.writeValueAsString(result)

    @Suppress("UNCHECKED_CAST")
    private fun parseMap(value: String?): Map<String, Any> =
        if (value == null || value.isNullOrBlank()|| value == "null") emptyMap()
        else {
            val v = value.trim('"',' ','\t','\n','\r')
            OBJECT_MAPPER.readValue(v, Map::class.java) as Map<String, Any>
        }

}
