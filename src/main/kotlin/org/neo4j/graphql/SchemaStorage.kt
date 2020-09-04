package org.neo4j.graphql

import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.Label

/**
 * @author mh
 * *
 * @since 29.10.16
 */
object SchemaStorage {

    val LABEL = Label.label("GraphQLSchema")
    val PROPERTY = "database"

    fun createConstraint(dbms: DatabaseManagementService) {
        dbms.database("system").beginTx()
                .use { tx ->
                    try {
                        tx.schema().getConstraintByName("GraphQLSchema_Unique")
                    } catch (ex: IllegalArgumentException) {
                        tx.schema().constraintFor(LABEL).assertPropertyIsUnique(PROPERTY).withName("GraphQLSchema_Unique")
                    }
                    tx.commit()
                }
    }

    fun deleteSchema(dbms: DatabaseManagementService, dbName: String): Unit = dbms.database("system").beginTx()
            .use { tx -> tx.findNode(LABEL, PROPERTY, dbName)?.let { it.delete(); tx.commit() } }

    fun schemaProperties(dbms: DatabaseManagementService, dbName: String): Map<String, Any> = dbms.database("system").beginTx()
            .use { tx ->
                return tx.findNode(LABEL, PROPERTY, dbName)?.allProperties ?: emptyMap()
            }

    fun updateSchema(dbms: DatabaseManagementService, dbName: String, schema: String): Unit = dbms.database("system").beginTx()
            .use { tx ->
                tx.execute("MERGE (g:$LABEL {$PROPERTY:\$database}) SET g.schema=\$schema, g.lastUpdated=timestamp()",
                        mapOf("database" to dbName, "schema" to schema))
                        .close()
                tx.commit()
            }
}
