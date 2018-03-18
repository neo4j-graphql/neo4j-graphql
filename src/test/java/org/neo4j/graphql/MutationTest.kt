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
class MutationTest {
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
type Movie  {
  title: String!
  released: Int
  actors: [Person] @relation(name:"ACTED_IN",direction:IN)
  directors: [Person] @cypher(statement:"MATCH (this)<-[:DIRECTED]-(d) RETURN d")
}
type Person {
  name: String!
  born: Int
  movies: [Movie] @relation(name:"ACTED_IN")
}
type Director {
  id: ID!
  name: String!
  born: Int
}
schema {
   query: QueryType
   mutation: MutationType
}
type QueryType {
  coActors(name:ID!): [Person] @cypher(statement:"MATCH (p:Person {name:{name}})-[:ACTED_IN]->()<-[:ACTED_IN]-(co) RETURN distinct co")
}
type MutationType {
  rateMovie(user:ID!, movie:ID!, rating:Int!): Int
  @cypher(statement:"MATCH (p:Person {name:{user}}),(m:Movie {title:{movie}}) MERGE (p)-[r:RATED]->(m) SET r.rating={rating} RETURN r.rating")
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
    fun createDirector() {
        val result = graphQL!!.execute("""mutation { d: createDirector(id:"123", name:"Lilly Wachowski" born:1967) }""", ctx)
        if (result.errors.isNotEmpty()) println(result.errors)
        assertEquals(mapOf("d" to
                "Nodes created: 1\nProperties set: 3\nLabels added: 1\n"), result.getData())
    }
    @Test
    fun createMovie() {
        val result = graphQL!!.execute("""mutation { m: createMovie(title:"Forrest Gump", released:1994) }""", ctx)
        if (result.errors.isNotEmpty()) println(result.errors)
        assertEquals(mapOf("m" to
                "Nodes created: 1\nProperties set: 2\nLabels added: 1\n"), result.getData())
    }
    @Test
    fun addActors() {
        val result = graphQL!!.execute("""mutation {
         m: createMovie(title:"Forrest Gump", released:1994)
         a: createPerson(name:"Tom Hanks", born:1954)
         cast: addMovieActors(title:"Forrest Gump", actors:["Tom Hanks"])}""", ctx)
        if (result.errors.isNotEmpty()) println(result.errors)
        assertEquals(
                mapOf("m" to "Nodes created: 1\nProperties set: 2\nLabels added: 1\n",
                "a" to "Nodes created: 1\nProperties set: 2\nLabels added: 1\n",
                "cast" to "Relationships created: 1\n"), result.getData())
    }
    @Test
    fun rateMovie() {
        db!!.execute("CREATE (:Movie {title:'Forrest Gump'}),(:Person {name:'Michael'})").close()
        val result = graphQL!!.execute("""mutation { r: rateMovie(movie:"Forrest Gump", user:"Michael", rating: 5) }""", ctx)
        if (result.errors.isNotEmpty()) println(result.errors)
        assertEquals(
                mapOf("r" to 5), result.getData())
    }
}
