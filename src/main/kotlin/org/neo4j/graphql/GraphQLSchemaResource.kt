package org.neo4j.graphql

import graphql.DeferredExecutionResultImpl.newDeferredExecutionResult
import graphql.GraphqlErrorBuilder
import graphql.schema.idl.SchemaPrinter
import graphql.schema.idl.errors.SchemaProblem
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.logging.LogProvider
import javax.ws.rs.*
import javax.ws.rs.core.Context
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

class GraphQLSchemaResource(
        @Context val provider: LogProvider,
        @Context val dbms: DatabaseManagementService,
        @Suppress("UnresolvedRestParam") @PathParam("db") val dbName: String
) {

    @POST
    @Consumes(APPLICATION_GRAPHQL)
    fun createSchemaFromIdl(schema: String): Response {
        return try {
            val augmentedSchema = SchemaStorage.updateSchema(dbms, dbName, schema)
            return Response.ok(SCHEMA_PRINTER.print(augmentedSchema), APPLICATION_GRAPHQL_TYPE)
                    .build()
        } catch (e: SchemaProblem) {
            Response.status(Response.Status.BAD_REQUEST)
                    .entity(newDeferredExecutionResult().addErrors(e.errors).build())
                    .type(MediaType.APPLICATION_JSON)
                    .build()
        } catch (e: Exception) {
            Response.serverError()
                    .entity(newDeferredExecutionResult().addErrors(listOf(GraphqlErrorBuilder
                            .newError()
                            .message(e.message)
                            .extensions(mapOf("trace" to e.stackTraceAsString()))
                            .build())).build())
                    .type(MediaType.APPLICATION_JSON)
                    .build()
        }
    }

    @GET
    @Produces(APPLICATION_GRAPHQL)
    fun getSchema(): Response {
        return SchemaStorage.getSchema(dbms, dbName)?.let { schema ->
            return Response.ok(schema, APPLICATION_GRAPHQL_TYPE).build()
        } ?: Response.status(Response.Status.NOT_FOUND).build()
    }

    @GET
    @Path("/augmented")
    @Produces(APPLICATION_GRAPHQL)
    fun getAugmentedSchema(): Response {
        return SchemaStorage.getAugmentedSchema(dbms, dbName)?.let { augmentedSchema ->
            return Response.ok(SCHEMA_PRINTER.print(augmentedSchema), APPLICATION_GRAPHQL_TYPE)
                    .build()
        } ?: Response.status(Response.Status.NOT_FOUND).build()
    }

    @DELETE
    fun deleteSchema(): Response = when (SchemaStorage.deleteSchema(dbms, dbName)) {
        true -> Response.noContent().build()
        false -> Response.status(Response.Status.NOT_FOUND).build()
    }

    companion object {
        val APPLICATION_GRAPHQL_TYPE = MediaType("application", "graphql")
        const val APPLICATION_GRAPHQL = "application/graphql"
        val SCHEMA_PRINTER = SchemaPrinter(SchemaPrinter.Options.defaultOptions()
                .descriptionsAsHashComments(true)
                .includeIntrospectionTypes(true)
                .includeSchemaDefinition(true)
                .useAstDefinitions(true))
    }
}
