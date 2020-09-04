package org.neo4j.graphql

import apoc.cypher.CypherFunctions
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import kotlin.test.assertEquals

object TestUtil {
    var db: GraphDatabaseService? = null
    var dbms: DatabaseManagementService? = null
    val dbName = "neo4j"
    init {
        dbms = TestDatabaseManagementServiceBuilder().impermanent().build()
        db = dbms!!.database("neo4j")
        (db as GraphDatabaseAPI).dependencyResolver.resolveDependency(GlobalProcedures::class.java).let {
            it.registerFunction(GraphQLProcedure::class.java)
            it.registerFunction(CypherFunctions::class.java)
            it.registerProcedure(GraphQLProcedure::class.java)
        }
    }

    fun setup(schema:String) {
        SchemaStorage.updateSchema(dbms!!, dbName, schema)
    }
    fun shutdown() {
        dbms!!.shutdown()
    }
    fun tearDown() {
        execute("MATCH (n) DETACH DELETE n")
        SchemaStorage.deleteSchema(dbms!!, dbName)
    }
    fun assertResult(query: String, expected: Any, variables: Map<String, Any>? = null, params: Map<String, Any?>? = null) {
        doExecute(variables?: emptyMap() , query) { cypher, res ->
            println(res)
            assertEquals(expected, res)
            if (params != null) {
                assertEquals(cypher.params, params)
            }
        }
        // if ((result?.get("errors") as List<*>?)?.isNotEmpty() ?: false)
    }

    private fun doExecute(variables: Map<String, Any>, query: String, cb: (Cypher, Map<String,Any?>) -> Unit): List<Map<String, Any?>> {
        db!!.beginTx().use { tx ->
            try {
                val schemaIdl = SchemaStorage.schemaProperties(dbms!!, db!!.databaseName()).get("schema") as String?
                        ?: throw IllegalStateException("No Schema available for " + db!!.databaseName())
                val schemaConfig = SchemaConfig()
                val schema = SchemaBuilder.buildSchema(schemaIdl, schemaConfig)
                val queries = Translator(schema).translate(query, variables, QueryContext())
                // todo handle operation, return query-key/alias
                val results = queries.map { cypher -> tx.execute(cypher.query, cypher.params).let { r ->
                    println(cypher.query)
                    val isList = cypher.type?.isList() ?: false
                    val col = r.columns().first()
                    val res = mapOf(col to r.columnAs<Any>(col).asSequence().let { if (isList) it.toList() else it.firstOrNull() })
                    cb(cypher, res)
                    res
                } }
                tx.commit()
                return results
            } catch (e: Exception) {
                tx.rollback()
                throw RuntimeException("Error executing GraphQL Query: $query", e)
            }
        }
    }

    fun execute(stmt: String) = db!!.executeTransactionally(stmt)
}