package org.neo4j.graphql

import graphql.GraphQL
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
class FilterTest {
    private var db: GraphDatabaseService? = null
    private var ctx: GraphQLContext? = null
    private var graphQL: GraphQL? = null


    @Before
    @Throws(Exception::class)
    fun setUp() {
        db = TestGraphDatabaseFactory().newImpermanentDatabase()
        (db as GraphDatabaseAPI).dependencyResolver.resolveDependency(Procedures::class.java).let {
            it.registerFunction(GraphQLProcedure::class.java)
            it.registerProcedure(GraphQLProcedure::class.java)
        }
        db?.execute("CREATE (c:Company {name:'ACME'}) WITH c UNWIND [{id:'jane',name:'Jane', age:38, gender:'female',fun:true, height:1.75},{id:'joe',name:'Joe', age:42, gender:'male',fun:false, height:1.85}] AS props CREATE (p:Person)-[:WORKS_AT]->(c) SET p = props")?.close()

        ctx = GraphQLContext(db!!)
        GraphSchemaScanner.storeIdl(db!!, schema)
        graphQL = GraphSchema.getGraphQL(db!!)
    }

    val schema = """
enum Gender { female, male }
type Person {
    id : ID!
    name: String
    age: Int
    height: Float
    fun: Boolean
    gender: Gender
    company: Company @relation(name:"WORKS_AT")
}
type Company {
    name: String
    employees: [Person] @relation(name:"WORKS_AT", direction: IN)
}
"""

    @After
    @Throws(Exception::class)
    fun tearDown() {
        db?.shutdown()
    }

    val jane = mapOf("p" to listOf(mapOf("name" to "Jane")))
    val joe = mapOf("p" to listOf(mapOf("name" to "Joe")))

    fun assertFilter(filter: String, expected: String) {
        assertResult("""{ p: Person(filter: { $filter }) { name }}""", mapOf("p" to listOf(mapOf("name" to expected))))
    }

    private fun assertResult(query: String, expected: Any, params: Map<String,Any> = emptyMap()) {
        val result = graphQL!!.execute(query, ctx, params)
        if (result.errors.isNotEmpty()) println(result.errors)
        assertEquals(expected, result.getData())
    }

    @Test
    fun singleRelation() {
        assertResult("""{ p: Person(filter: { company : { name : "ACME" } }) { name }}""", mapOf("p" to listOf(mapOf("name" to "Jane"),mapOf("name" to "Joe"))))
        assertResult("""{ p: Person(filter: { company_not : { name : "ACME" } }) { name }}""", mapOf("p" to emptyList<Map<String,Any>>()))
    }

    @Test
    fun multiRelation() {
        db?.execute("CREATE (c:Company {name:'ACME2'}) WITH c UNWIND [{id:'jill',name:'Jill', age:32, gender:'female',fun:true, height:1.65}] AS props CREATE (p:Person)-[:WORKS_AT]->(c) SET p = props")?.close()

        assertResult("""{ p: Company(filter: { employees : { name_in : ["Jane","Joe"] } }) { name }}""", mapOf("p" to listOf(mapOf("name" to "ACME"))))
        assertResult("""{ p: Company(filter: { employees_some : { name : "Jane" } }) { name }}""", mapOf("p" to listOf(mapOf("name" to "ACME"))))
        assertResult("""{ p: Company(filter: { employees_every : { name : "Jill" } }) { name }}""", mapOf("p" to listOf(mapOf("name" to "ACME2"))))
        assertResult("""{ p: Company(filter: { employees_some : { name : "Jill" } }) { name }}""", mapOf("p" to listOf(mapOf("name" to "ACME2"))))
        assertResult("""{ p: Company(filter: { employees_none : { name : "Jane" } }) { name }}""", mapOf("p" to listOf(mapOf("name" to "ACME2"))))
        assertResult("""{ p: Company(filter: { employees_none : { name : "Jill" } }) { name }}""", mapOf("p" to listOf(mapOf("name" to "ACME"))))
        assertResult("""{ p: Company(filter: { employees_single : { name : "Jill" } }) { name }}""", mapOf("p" to listOf(mapOf("name" to "ACME2"))))
    }

    @Test
    fun parameterFilter() {
        val params = mapOf("filter" to mapOf("name" to "Jane"))
        val query = """query filterQuery(${"$"}filter: _PersonFilter) { p: Person(filter: ${"$"}filter) { name }}"""
        assertResult(query, mapOf("p" to listOf(mapOf("name" to "Jane"))), params)
    }
    @Test
    fun parameterRelationFilter() {
        val params = mapOf("filter" to mapOf("AND" to mapOf("name" to "Jane", "company" to mapOf("name_ends_with" to "ME"))))
        val query = """query filterQuery(${"$"}filter: _PersonFilter) { p: Person(filter: ${"$"}filter) { name }}"""
        assertResult(query, mapOf("p" to listOf(mapOf("name" to "Jane"))), params)
    }

    @Test
    fun fieldFilter() {
        assertResult("""{ p: Company { employees(filter: { name: "Jane" }) { name }}}""", mapOf("p" to listOf(mapOf("employees" to listOf(mapOf("name" to "Jane"))))))
        assertResult("""{ p: Company { employees(filter: { OR: [{ name: "Jane" },{name:"Joe"}]}) { name }}}""", mapOf("p" to listOf(mapOf("employees" to listOf(mapOf("name" to "Joe"), mapOf("name" to "Jane"))))))
    }
    @Test
    fun stringFilter() {
        assertFilter("name: \"Jane\"", "Jane")
        assertFilter("name_starts_with: \"Ja\"", "Jane")
        assertFilter("name_not_starts_with: \"Ja\"", "Joe")
        assertFilter("name_ends_with: \"ne\"", "Jane")
        assertFilter("name_not_ends_with: \"ne\"", "Joe")
        assertFilter("name_contains: \"an\"", "Jane")
        assertFilter("name_not_contains: \"an\"", "Joe")

        assertFilter("name_in: [\"Jane\"]", "Jane")
        assertFilter("name_not_in: [\"Joe\"]", "Jane")
        assertFilter("name_not: \"Joe\"", "Jane")
    }

    @Test
    fun idFilter() {
        assertFilter("id: \"jane\"", "Jane")
        assertFilter("id_starts_with: \"ja\"", "Jane")
        assertFilter("id_not_starts_with: \"ja\"", "Joe")
        assertFilter("id_ends_with: \"ne\"", "Jane")
        assertFilter("id_not_ends_with: \"ne\"", "Joe")
        assertFilter("id_contains: \"an\"", "Jane")
        assertFilter("id_not_contains: \"an\"", "Joe")

        assertFilter("id_in: [\"jane\"]", "Jane")
        assertFilter("id_not_in: [\"joe\"]", "Jane")
        assertFilter("id_not: \"joe\"", "Jane")

    }

    @Test
    fun intFilter() {
        assertFilter("age: 38", "Jane")
        assertFilter("age_in: [38]", "Jane")
        assertFilter("age_not_in: [38]", "Joe")
        assertFilter("age_lte: 40", "Jane")
        assertFilter("age_lt: 40", "Jane")
        assertFilter("age_gt: 40", "Joe")
        assertFilter("age_gte: 40", "Joe")

    }

    @Test
    fun enumFilter() {
        assertFilter("gender: male", "Joe")
        assertFilter("gender_not: male", "Jane")
        assertFilter("gender_not_in: [male]", "Jane")
        assertFilter("gender_in: [male]", "Joe")

    }

    @Test
    fun booleanFilter() {
        assertFilter("fun: true", "Jane")
        assertFilter("fun_not: true", "Joe")
    }
    @Test
    fun andFilter() {
        assertFilter("""AND: [{ fun: true, name: "Jane"}] """, "Jane")
        assertFilter("""AND: [{ fun: true},{name: "Jane"}] """, "Jane")
    }

    @Test
    fun orFilter() {
        assertFilter("""OR: [{ fun: false, name_not: "Jane"}] """, "Joe")
        assertFilter("""OR: [{ fun: true},{name_in: ["Jane"]}] """, "Jane")
    }
    @Test
    fun orAndFilter() {
        assertFilter("""OR: [{ AND: [{fun: true},{height:1.75}]},{name_in: ["Jane"]}] """, "Jane")
    }

    @Test
    fun floatFilter() {
        assertFilter("height: 1.75", "Jane")
        assertFilter("height_not: 1.75", "Joe")
        assertFilter("height_in: [1.75]", "Jane")
        assertFilter("height_not_in: [1.75]", "Joe")
        assertFilter("height_lte: 1.80", "Jane")
        assertFilter("height_lt: 1.80", "Jane")
        assertFilter("height_gte: 1.80", "Joe")
        assertFilter("height_gt: 1.80", "Joe")
    }
}
