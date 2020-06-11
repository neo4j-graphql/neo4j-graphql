package org.neo4j.graphql

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
    fun assertResult(query: String, expected: Any) {
        val result = db!!.executeTransactionally("call graphql.execute(\$query)", mapOf("query" to query)) { it.asSequence().firstOrNull()?.get("result") as Map<*, *>? }
        println(result)
        // if ((result?.get("errors") as List<*>?)?.isNotEmpty() ?: false)
        assertEquals(expected, (result?.get("data") as List<Any>?)?.first())
    }

    fun execute(stmt: String) = db!!.executeTransactionally(stmt)
}