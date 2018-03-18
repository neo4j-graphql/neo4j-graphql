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

class ScalarTest {
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
        ctx = GraphQLContext(db!!)
        GraphSchemaScanner.storeIdl(db!!, schema)
        graphQL = GraphSchema.getGraphQL(db!!)
    }

    val schema = """
scalar Date
type Movie  {
  title: String!
  released: Date
}
"""

    @After
    @Throws(Exception::class)
    fun tearDown() {
        db?.shutdown()
    }

    private fun assertResult(query: String, expected: Any) {
        val result = graphQL!!.execute(query, ctx)
        if (result.errors.isNotEmpty()) println(result.errors)
        assertEquals(expected, result.getData())
    }

    @Test
    fun createMovie() {
        val result = graphQL!!.execute("""mutation { m: createMovie(title:"Forrest Gump", released:1994) }""", ctx)
        if (result.errors.isNotEmpty()) println(result.errors)
        assertEquals(mapOf("m" to
                "Nodes created: 1\nProperties set: 2\nLabels added: 1\n"), result.getData())
    }

    @Test
    fun updateMovie() {
        createMovieData()
        val result = graphQL!!.execute("""mutation { m: updateMovie(title:"Forrest Gump", released:1995) }""", ctx)
        if (result.errors.isNotEmpty()) println(result.errors)
        assertEquals(mapOf("m" to
                "Properties set: 1\n"), result.getData())
    }

    @Test
    fun findMovie() {
        createMovieData()
        var result = graphQL!!.execute("""{ Movie(title:"Forrest Gump") { title, released } }""", ctx)
        assertEquals(mapOf("Movie" to listOf(mapOf("title" to "Forrest Gump", "released" to 1994L))), result.getData(), result.errors.toString())
        result = graphQL!!.execute("""{ Movie(released:1994) { title, released } }""", ctx)
        assertEquals(mapOf("Movie" to listOf(mapOf("title" to "Forrest Gump", "released" to 1994L))), result.getData(), result.errors.toString())
    }

    @Test
    fun findMovieFilter() {
        createMovieData()
        val result = graphQL!!.execute("""{ Movie(filter:{released_gte:1994}) { title, released } }""", ctx)
        assertEquals(mapOf("Movie" to listOf(mapOf("title" to "Forrest Gump", "released" to 1994L))), result.getData(), result.errors.toString())
    }

    private fun createMovieData() {
        db!!.execute("CREATE (:Movie {title:'Forrest Gump', released:1994})").close()
    }
}
