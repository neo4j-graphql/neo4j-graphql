package org.neo4j.graphql

import graphql.GraphQL
import org.neo4j.graphdb.GraphDatabaseService
import java.util.concurrent.atomic.AtomicLong

/**
 * @author mh
 * *
 * @since 29.10.16
 */
object GraphSchema {
    private var graphql: GraphQL? = null
    private var lastSchemaElements = 0
    private val lastUpdated : AtomicLong = AtomicLong()
    private val lastCheck : AtomicLong = AtomicLong()
    private val UPDATE_FREQ = 10_000

    @JvmStatic fun getGraphQL(db: GraphDatabaseService): GraphQL {
        val schemaElements = countSchemaElements(db)
        if (graphql == null || lastSchemaElements != schemaElements || needUpdate(db)) {
            lastSchemaElements = schemaElements
            val graphQLSchema = GraphQLSchemaBuilder.buildSchema(db)
            graphql = GraphQL.newGraphQL(graphQLSchema).build()
            lastUpdated.set(System.currentTimeMillis())
        }
        return graphql!!
    }

    private fun needUpdate(db: GraphDatabaseService): Boolean {
        val now = System.currentTimeMillis()
        if (now - lastCheck.getAndSet(now) < UPDATE_FREQ) return false
        return (GraphSchemaScanner.readIdlUpdate(db) > lastUpdated.get())
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

    @JvmStatic fun reset() {
        graphql = null
    }
}
