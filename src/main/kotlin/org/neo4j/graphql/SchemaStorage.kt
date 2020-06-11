package org.neo4j.graphql

import graphql.GraphQL
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import java.util.concurrent.atomic.AtomicLong

/**
 * @author mh
 * *
 * @since 29.10.16
 */
object SchemaStorage {

    fun deleteSchema(dbms: DatabaseManagementService, dbName: String): Unit = dbms.database("system").beginTx()
            .use { tx -> tx.findNode(Label.label("GraphQLSchema"), "database", dbName)?.let { it.delete(); tx.commit() } }

    fun schemaProperties(dbms: DatabaseManagementService, dbName: String): Map<String, Any> = dbms.database("system").beginTx()
            .use { tx ->
                return tx.findNode(Label.label("GraphQLSchema"), "database", dbName)?.allProperties ?: emptyMap()
            }

    fun updateSchema(dbms: DatabaseManagementService, dbName: String, schema: String): Unit = dbms.database("system").beginTx()
            .use { tx ->
                (tx.findNode(Label.label("GraphQLSchema"), "database", dbName)
                        ?: tx.createNode(Label.label("GraphQLSchema")).also { it.setProperty("database", dbName); }
                        ).also { it.setProperty("schema", schema); it.setProperty("lastUpdated", System.currentTimeMillis()); tx.commit() }
            }
}
