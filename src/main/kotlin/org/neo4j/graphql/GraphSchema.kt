package org.neo4j.graphql

import graphql.GraphQL
import org.neo4j.graphdb.GraphDatabaseService

/**
 * @author mh
 * *
 * @since 29.10.16
 */
object GraphSchema {
    private var graphql: GraphQL? = null

    @JvmStatic fun getGraphQL(db: GraphDatabaseService): GraphQL {
        if (graphql == null) {
            val graphQLSchema = GraphQLSchemaBuilder.buildSchema(db)
            graphql = GraphQL(graphQLSchema)
        }
        return graphql!!
    }
}
