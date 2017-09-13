package org.neo4j.graphql

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import kotlin.test.assertEquals

/**
 * @author mh
 * *
 * @since 05.05.17
 */
class DynamicTypesTest {
    private var db: GraphDatabaseService? = null

    @Before
    @Throws(Exception::class)
    fun setUp() {
        db = TestGraphDatabaseFactory().newImpermanentDatabase()
        (db as GraphDatabaseAPI).dependencyResolver.resolveDependency(Procedures::class.java).let {
            it.registerFunction(GraphQLProcedure::class.java)
            it.registerProcedure(GraphQLProcedure::class.java)
        }
        db?.execute("CREATE (:Person {name:'Jane'})-[:KNOWS]->(:Person {name:'John'})")?.close()
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        db?.shutdown()
    }

    @Test
    fun dynamicQueryTypeAccessAttributes() {
        val schema = """
        type Person {
           name: String
           knows: [Person] @relation(name:"KNOWS")
        }
        type QueryType {
            people: [Person] @cypher(statement: "MATCH (p:Person) RETURN p")
        }
        schema { query : QueryType } """

        GraphSchemaScanner.storeIdl(db!!, schema)
        val graphQL = GraphSchema.getGraphQL(db!!)
        val result = graphQL.execute("{ people { name, _id }}", GraphQLContext(db!!))
        assertEquals(mapOf("people" to listOf(mapOf("name" to "Jane","_id" to 0L),mapOf("name" to "John","_id" to 1L))),result.getData())
    }

    @Test
    fun dynamicQueryTypeAccessNestedAttributes() {
        val schema = """
        type Person {
           name: String
           knows: [Person] @relation(name:"KNOWS")
        }
        type QueryType {
            people: [Person] @cypher(statement: "MATCH (p:Person) RETURN p")
        }
        schema { query : QueryType } """

        GraphSchemaScanner.storeIdl(db!!, schema)
        val graphQL = GraphSchema.getGraphQL(db!!)
        val result = graphQL.execute("{ people { name, knows { name } }}", GraphQLContext(db!!))
        assertEquals(mapOf("people" to listOf(
                mapOf("name" to "Jane","knows" to listOf(mapOf<String,Any>("name" to "John"))),
                mapOf("name" to "John","knows" to emptyList<Map<String,Any>>()))),result.getData())
    }

    @Test
    fun dynamicQueryTypeAccessNestedAttributesNestedQuery() {
        val schema = """
        type Person {
           name: String
           knows: [Person] @relation(name:"KNOWS")
        }
        type QueryType {
            people: [Person] @cypher(statement: "MATCH (p:Person) RETURN p")
        }
        schema { query : QueryType } """

        GraphSchemaScanner.storeIdl(db!!, schema)
        val graphQL = GraphSchema.getGraphQL(db!!)
        val result = graphQL.execute("{ people { name, knows(name:\"John\") { name } }}", GraphQLContext(db!!))
        assertEquals(mapOf("people" to listOf(
                mapOf("name" to "Jane","knows" to listOf(mapOf<String,Any>("name" to "John"))),
                mapOf("name" to "John","knows" to emptyList<Map<String,Any>>()))),result.getData())
    }

    @Test
    fun dynamicParameterizedQueryTypeAccessNestedAttributes() {
        val schema = """
        type Person {
           name: String
           knows: [Person] @relation(name:"KNOWS")
        }
        type QueryType {
            people(name:String): [Person] @cypher(statement: "MATCH (p:Person {name:{name}}) RETURN p")
        }
        schema { query : QueryType } """

        GraphSchemaScanner.storeIdl(db!!, schema)
        val graphQL = GraphSchema.getGraphQL(db!!)
        val result = graphQL.execute("{ people(name:\"Jane\") { name, knows { name } }}", GraphQLContext(db!!))
        assertEquals(mapOf("people" to listOf(
                mapOf("name" to "Jane", "knows" to listOf(mapOf<String, Any>("name" to "John")))))
                , result.getData())
    }

    @Test
    fun dynamicMutationTypeAccessAttributes() {
        val schema = """
        type Person {
           name: String
           knows: [Person] @relation(name:"KNOWS")
        }
        type MutationType {
            newPerson(name:String) : Person @cypher(statement: "CREATE (p:Person {name:{name}}) RETURN p")
        }
        schema { mutation : MutationType }"""

        GraphSchemaScanner.storeIdl(db!!, schema)
        val graphQL = GraphSchema.getGraphQL(db!!)
        val result = graphQL.execute("mutation createData { person: newPerson(name:\"Alice\") { name, _id }}", GraphQLContext(db!!))
        println(result)
        assertEquals(mapOf("person" to mapOf("name" to "Alice","_id" to 2L)),result.getData())
    }
    @Test
    fun dynamicMutationTypeAccessNestedAttributes() {
        val schema = """
        type Person {
           name: String
           knows: [Person] @relation(name:"KNOWS")
        }
        type MutationType {
            obtainPerson(name:String) : Person @cypher(statement: "MERGE (p:Person {name:{name}}) RETURN p")
        }
        schema { mutation : MutationType }"""

        GraphSchemaScanner.storeIdl(db!!, schema)
        val graphQL = GraphSchema.getGraphQL(db!!)
        val result = graphQL.execute("mutation createData { person: obtainPerson(name:\"Jane\") { name, knows { name } }}", GraphQLContext(db!!))
        println(result)
        assertEquals(mapOf("person" to mapOf("name" to "Jane","knows" to listOf(mapOf<String,Any>("name" to "John")))),result.getData())
    }
}
