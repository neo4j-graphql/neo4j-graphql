package org.neo4j.graphql

import org.codehaus.jackson.map.ObjectMapper
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.Log
import org.neo4j.logging.LogProvider
import java.io.IOException
import javax.ws.rs.*
import javax.ws.rs.core.Context
import javax.ws.rs.core.HttpHeaders
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

/**
 * @author mh
 * @since 30.10.16
 */
@Path("/")
class GraphQLResource(@Context val provider: LogProvider, @Context val db: GraphDatabaseService) {
    val log: Log
    init {
        log = provider.getLog(GraphQLResource::class.java)
    }

    companion object {
        val OBJECT_MAPPER: ObjectMapper = ObjectMapper()
    }

    @Path("/")
    @OPTIONS
    fun options(@Context headers: HttpHeaders): Response {
        val origin = headers.getRequestHeader("Origin").firstOrNull()
        return if (origin.isNullOrEmpty()) Response.ok().build()
        else Response.ok().header("Access-Control-Allow-Origin", origin).build()
    }

    @Path("/")
    @GET
    operator fun get(@QueryParam("query") query: String, @QueryParam("variables") variableParam: String): Response {
        return executeQuery(hashMapOf("query" to query, "variables" to variableParam))
    }

    @Path("/")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    fun executeOperation(body: String): Response {
        return executeQuery(parseMap(body))
    }

    private fun executeQuery(params: Map<String, Any>): Response {
        val query = params["query"] as String
        val variables = getVariables(params)

        val ctx = GraphQLContext(db, log)
        val executionResult = GraphSchema.getGraphQL(db).execute(query, ctx, variables)

        val result = linkedMapOf("data" to executionResult.getData<Any>())
        if (executionResult.errors.isNotEmpty()) {
            log.error("Errors: {}", executionResult.errors)
            result.put("errors", executionResult.errors)
        }
        if (ctx.backLog.isNotEmpty()) {
            result["extensions"]=ctx.backLog
        }
        return Response.ok().entity(formatMap(result)).build()
    }

    @Suppress("UNCHECKED_CAST")
    private fun getVariables(requestBody: Map<String, Any>): Map<String, Any> {
        val varParam = requestBody["variables"]
        return when (varParam) {
            is String -> if (varParam.isNotBlank()) parseMap(varParam) else emptyMap()
            is Map<*, *> -> varParam as Map<String, Any>
            else -> emptyMap()
        }
    }

    private fun formatMap(result: Map<String, Any>) = OBJECT_MAPPER.writeValueAsString(result)

    @Suppress("UNCHECKED_CAST")
    private fun parseMap(value: String): Map<String, Any> = OBJECT_MAPPER.readValue(value, Map::class.java) as Map<String, Any>

}
