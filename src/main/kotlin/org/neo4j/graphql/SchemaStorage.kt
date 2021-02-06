package org.neo4j.graphql

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import graphql.schema.GraphQLSchema
import graphql.schema.idl.errors.SchemaProblem
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.Label
import java.time.Duration
import java.time.temporal.ChronoUnit

/**
 * @author mh
 * *
 * @since 29.10.16
 */
object SchemaStorage {

    private val LABEL = Label.label("GraphQLSchema")
    private const val DATABASE_PROPERTY = "database"
    private const val SCHEMA_PROPERTY = "schema"
    private val SCHEMA_CACHE: Cache<String, GraphQLSchema> = Caffeine.newBuilder()
            .expireAfterAccess(Duration.of(30, ChronoUnit.MINUTES))
            .build()

    fun createConstraint(dbms: DatabaseManagementService) {
        dbms.database("system").beginTx()
                .use { tx ->
                    try {
                        tx.schema().getConstraintByName("GraphQLSchema_Unique")
                    } catch (ex: IllegalArgumentException) {
                        tx.schema().constraintFor(LABEL).assertPropertyIsUnique(DATABASE_PROPERTY).withName("GraphQLSchema_Unique")
                    }
                    tx.commit()
                }
    }

    fun deleteSchema(dbms: DatabaseManagementService, dbName: String): Boolean = dbms.database("system")
            .beginTx().use { tx ->
                tx.findNode(LABEL, DATABASE_PROPERTY, dbName)?.let { node ->
                    node.getProperty(SCHEMA_PROPERTY)?.also { SCHEMA_CACHE.invalidate(it) }
                    node.delete()
                    tx.commit()
                    true
                } ?: false
            }

    private fun schemaProperties(dbms: DatabaseManagementService, dbName: String): Map<String, Any> = dbms.database("system").beginTx()
            .use { tx ->
                return tx.findNode(LABEL, DATABASE_PROPERTY, dbName)?.allProperties ?: emptyMap()
            }

    fun getSchema(dbms: DatabaseManagementService, dbName: String) = schemaProperties(dbms, dbName)[SCHEMA_PROPERTY] as String?

    fun getAugmentedSchema(dbms: DatabaseManagementService, dbName: String) = getSchema(dbms, dbName)
            ?.let { schema -> SCHEMA_CACHE.get(schema, SchemaStorage::augmentSchema) }

    @Throws(SchemaProblem::class)
    fun updateSchema(dbms: DatabaseManagementService, dbName: String, schema: String): GraphQLSchema {
        val augmentedSchema = augmentSchema(schema)
        dbms.database("system")
                .beginTx()
                .use { tx ->
                    (tx.findNode(LABEL, DATABASE_PROPERTY, dbName)
                            ?: tx.createNode(LABEL).also { it.setProperty(DATABASE_PROPERTY, dbName); })
                            .also {

                                it.setProperty(SCHEMA_PROPERTY, schema)
                                it.setProperty("lastUpdated", System.currentTimeMillis())
                                tx.commit()
                            }
                }
        SCHEMA_CACHE.put(schema, augmentedSchema)
        return augmentedSchema
    }

    @Throws(SchemaProblem::class)
    private fun augmentSchema(schema: String) =
        SchemaBuilder.buildSchema(schema, SchemaConfig(capitalizeQueryFields = false))
}
