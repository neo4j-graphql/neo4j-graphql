package org.neo4j.graphql

import graphql.GraphQL
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.Log
import org.neo4j.logging.LogProvider
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
@Path("/experimental")
class GraphQLResourceExperimental(@Context val provider: LogProvider, @Context val db: GraphDatabaseService) {
    val log: Log
    init {
        log = provider.getLog(GraphQLResourceExperimental::class.java)
    }

    companion object {
        val OBJECT_MAPPER = com.fasterxml.jackson.databind.ObjectMapper()
    }

    @Path("")
    @OPTIONS
    fun options(@Context headers: HttpHeaders) = Response.ok().build()

    @Path("")
    @GET
    fun get(@QueryParam("query") query: String?, @QueryParam("variables") variableParam: String?): Response {
        if (query == null) return Response.noContent().build()
        return executeQuery(hashMapOf("query" to query, "variables" to (variableParam ?: emptyMap<String,Any>())))
    }

    @Path("")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    fun executeOperation(body: String): Response {
        return executeQuery(parseMap(body))
    }

    private fun executeQuery(params: Map<String, Any>): Response {
        val query = params["query"] as String
        val variables = getVariables(params)
        if (log.isDebugEnabled()) log.debug("Executing {} with {}", query, variables)
        val tx = db.beginTx()
        try {
            val schemaIdl = GraphSchemaScanner.readIdl(db)!!
            val transpilerCtx = Translator.Context(topLevelWhere = false)
            val schema = SchemaBuilder.buildSchema(schemaIdl, transpilerCtx)

            val result : Any =
            if (query.contains("__schema")) {
                GraphQL.newGraphQL(schema).build().execute(query)
            } else {
                val queries = Translator(schema).translate(query, variables, transpilerCtx)
                // todo return query-key/alias
                val results = queries.flatMap { cypher -> db.execute(cypher.query, cypher.params).stream().toList() }
                linkedMapOf("data" to results)
            }
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
            tx.success()
            return Response.ok().entity(formatMap(result)).build()
        } finally {
            tx.close()
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
