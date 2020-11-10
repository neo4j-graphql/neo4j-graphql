package org.neo4j.graphql

import com.fasterxml.jackson.databind.ObjectMapper
import graphql.DeferredExecutionResultImpl
import graphql.GraphqlErrorBuilder
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.logging.Log
import org.neo4j.logging.LogProvider
import java.io.PrintWriter
import java.io.StringWriter
import javax.ws.rs.*
import javax.ws.rs.container.ResourceContext
import javax.ws.rs.core.Context
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

/**
 * @author mh
 * @since 30.10.16
 */
@Path("/{db}")
class GraphQLResource(
        @Context val provider: LogProvider,
        @Context val dbms: DatabaseManagementService,
        @Context val resourceContext: ResourceContext,
        @PathParam("db") val dbName: String
) {
    private val log: Log = provider.getLog(GraphQLResource::class.java)

    companion object {
        val OBJECT_MAPPER: ObjectMapper = ObjectMapper()
    }

    @Path("/idl")
    fun getIdlResource(): GraphQLSchemaResource = resourceContext.getResource(GraphQLSchemaResource::class.java)

    @OPTIONS
    fun options(): Response = Response.ok().build()

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    fun get(
            @QueryParam("query") query: String?,
            @QueryParam("variables") variableParam: String?
    ): Response {
        if (query == null) return Response.noContent().build()
        return executeQuery(mapOf("query" to query, "variables" to (variableParam ?: emptyMap<String, Any>())))
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    fun executeOperation(body: String): Response {
        return executeQuery(parseMap(body))
    }

    private fun Throwable.stackTraceAsString(): String {
        val sw = StringWriter()
        this.printStackTrace(PrintWriter(sw))
        return sw.toString()
    }


    private fun executeQuery(params: Map<String, Any>): Response {
        val query = params["query"] as String
        val variables = getVariables(params)
        if (log.isDebugEnabled) {
            log.debug("Executing {} with {}", query, variables)
        }
        return try {
            dbms.executeGraphQl(dbName, query, variables)
                    ?.let { Response.ok(it, MediaType.APPLICATION_JSON_TYPE).build() }
                    ?: return Response.status(Response.Status.PRECONDITION_REQUIRED)
                            .entity("No Schema available for $dbName")
                            .build()
        } catch (e: Exception) {
            log.warn("Errors: {}", e)
            Response.serverError()
                    .entity(DeferredExecutionResultImpl.newDeferredExecutionResult().addErrors(listOf(GraphqlErrorBuilder
                            .newError()
                            .message(e.message)
                            .extensions(mapOf("trace" to e.stackTraceAsString()))
                            .build())).build())
                    .type(MediaType.APPLICATION_JSON)
                    .build()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun getVariables(requestBody: Map<String, Any?>): Map<String, Any> {
        return when (val varParam = requestBody["variables"]) {
            is String -> parseMap(varParam)
            is Map<*, *> -> varParam as Map<String, Any>
            else -> emptyMap()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun parseMap(value: String?): Map<String, Any> =
            if (value == null || value.isNullOrBlank() || value == "null") emptyMap()
            else {
                val v = value.trim('"', ' ', '\t', '\n', '\r')
                OBJECT_MAPPER.readValue(v, Map::class.java) as Map<String, Any>
            }

}
