package org.neo4j.graphql

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

    @Path("/idl")
    fun getIdlResource(): GraphQLSchemaResource = resourceContext.getResource(GraphQLSchemaResource::class.java)

    @OPTIONS
    fun options(): Response = Response.ok().build()

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    fun get(
        @QueryParam("query") query: String?,
        @QueryParam("variables") variables: String?
    ): Response {
        if (query == null) return Response.noContent().build()
        return executeQuery(GraphQLRequest(query, variables))
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    fun executeOperation(request: GraphQLRequest): Response {
        return executeQuery(request)
    }

    private fun Throwable.stackTraceAsString(): String {
        val sw = StringWriter()
        this.printStackTrace(PrintWriter(sw))
        return sw.toString()
    }


    private fun executeQuery(request: GraphQLRequest): Response {
        if (log.isDebugEnabled) {
            log.debug("Executing {} with {}", request.query, request.variables)
        }
        return try {
            dbms.executeGraphQl(dbName, request.query, request.variables)
                ?.let { Response.ok(it, MediaType.APPLICATION_JSON_TYPE).build() }
                ?: return Response.status(Response.Status.PRECONDITION_REQUIRED)
                    .entity("No Schema available for $dbName")
                    .build()
        } catch (e: Exception) {
            log.warn("Errors: {}", e)
            Response.serverError()
                .entity(
                    DeferredExecutionResultImpl.newDeferredExecutionResult().addErrors(
                        listOf(
                            GraphqlErrorBuilder
                                .newError()
                                .message(e.message)
                                .extensions(mapOf("trace" to e.stackTraceAsString()))
                                .build()
                        )
                    ).build()
                )
                .type(MediaType.APPLICATION_JSON)
                .build()
        }
    }
}
