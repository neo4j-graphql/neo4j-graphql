package org.neo4j.graphql

import graphql.DeferredExecutionResultImpl
import graphql.ExecutionResult
import graphql.GraphQL
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService

fun DatabaseManagementService.executeGraphQl(
        databaseName: String,
        query: String,
        variables: Map<String, Any?> = emptyMap()
) = this.database(databaseName).executeGraphQl(this, query, variables)

fun GraphDatabaseService.executeGraphQl(
        dbms: DatabaseManagementService,
        query: String,
        variables: Map<String, Any?> = emptyMap()
): ExecutionResult? {
    val schema = SchemaStorage.getAugmentedSchema(dbms, this.databaseName())
            ?: return null
    return this.beginTx().use { tx ->
        try {
            if (query.contains("__schema")) {

                GraphQL.newGraphQL(schema).build().execute(query)

            } else {

                val queries = Translator(schema).translate(query, variables, QueryContext())
                val results = queries.map { cypher ->
                    tx.execute(cypher.query, cypher.params).let { r ->
                        val isList = cypher.type?.isList() ?: false
                        val col = r.columns().first()
                        mapOf(col to r.columnAs<Any>(col).asSequence().let { if (isList) it.toList() else it.firstOrNull() })
                    }
                }
                tx.commit()
                DeferredExecutionResultImpl.newExecutionResult().data(results).build()

            }
        } catch (e: Exception) {
            tx.rollback()
            throw e
        }
    }
}
