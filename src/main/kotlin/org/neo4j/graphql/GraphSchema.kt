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
    private var lastSchemaElements = 0

    @JvmStatic fun getGraphQL(db: GraphDatabaseService): GraphQL {
        val schemaElements = countSchemaElements(db)
        if (graphql == null || lastSchemaElements != schemaElements) {
            lastSchemaElements = schemaElements
            val graphQLSchema = GraphQLSchemaBuilder.buildSchema(db)
            graphql = GraphQL.newGraphQL(graphQLSchema).build()
        }
        return graphql!!
    }

    fun countSchemaElements(db: GraphDatabaseService): Int {
        val tx = db.beginTx()
        try {
            val count = db.allLabels.count() + db.allRelationshipTypes.count() + db.allPropertyKeys.count() +
                    db.schema().constraints.count() + db.schema().indexes.count()
            tx.success()
            return count
        } finally {
            tx.close()
        }
    }

    fun reset() {
        graphql = null;
    }
}
