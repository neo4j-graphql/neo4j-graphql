package org.neo4j.graphql

import org.junit.After
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.neo4j.graphql.TestUtil.assertResult
import org.neo4j.graphql.TestUtil.execute

/**
 * @author mh
 * *
 * @since 05.05.17
 */
class MutationTest {

    @Before
    @Throws(Exception::class)
    fun setUp() {
        TestUtil.setup(schema)
    }

    val schema = """
type Movie  {
  title: ID!
  released: Int
  actors: [Person] @relation(name:"ACTED_IN",direction:IN)
  directors: [Person] @cypher(statement:"MATCH (this)<-[:DIRECTED]-(d) RETURN d")
}
type Person {
  name: ID!
  born: Int
  movies: [Movie] @relation(name:"ACTED_IN")
}
type Director {
  id: ID!
  name: String!
  born: Int
}
schema {
   query: Query
   mutation: Mutation
}
type Query {
  coActors(name:ID!): [Person] @cypher(statement:"MATCH (p:Person {name:{name}})-[:ACTED_IN]->()<-[:ACTED_IN]-(co) RETURN distinct co")
}
type Mutation {
  rateMovie(user:ID!, movie:ID!, rating:Int!): Int
  @cypher(statement:"MATCH (p:Person {name:{user}}),(m:Movie {title:{movie}}) MERGE (p)-[r:RATED]->(m) SET r.rating={rating} RETURN r.rating as rating")
}
"""

    @After
    @Throws(Exception::class)
    fun tearDown() {
        TestUtil.tearDown()
    }

    @Test
    fun createDirector() {
        assertResult("""mutation { d: createDirector(id:"123", name:"Lilly Wachowski" born:1967) { id, name, born}}""",
                mapOf("d" to
                        mapOf("id" to "123", "name" to "Lilly Wachowski", "born" to 1967L)))
                
    }
    @Test
    fun createMovie() {
        assertResult("""mutation { m: createMovie(title:"Forrest Gump", released:1994) {title, released} }""",
                mapOf("m" to mapOf("title" to "Forrest Gump", "released" to 1994L)))
                
    }
    @Test
    fun mergeMovie() {
        assertResult("""mutation { m: mergeMovie(title:"Forrest Gump", released:1994) { released } }""",
                mapOf("m" to mapOf("released" to 1994L)))
                

        assertResult("""mutation { m: mergeMovie(title:"Forrest Gump", released:1995) { released }}""",
                mapOf("m" to mapOf("released" to 1995L)))
                
    }

    @Test
    @Ignore("https://github.com/neo4j-graphql/neo4j-graphql-java/issues/86")
    fun addActors() {
        assertResult("""mutation {
         m: createMovie(title:"Forrest Gump", released:1994) { title }
         a: createPerson(name:"Tom Hanks", born:1954) { born }
         cast: addMovieActors(title:"Forrest Gump", actors:["Tom Hanks"])} { _id }""",
                mapOf("m" to "Nodes created: 1\nProperties set: 2\nLabels added: 1\n",
                "a" to "Nodes created: 1\nProperties set: 2\nLabels added: 1\n",
                "cast" to "Relationships created: 1\n"))
                
    }
    @Test
    @Ignore("https://github.com/neo4j-graphql/neo4j-graphql-java/issues/85")
    fun rateMovie() {
        execute("CREATE (:Movie {title:'Forrest Gump'}),(:Person {name:'Michael'})")
        assertResult("""mutation { r: rateMovie(movie:"Forrest Gump", user:"Michael", rating: 5) }""",
                mapOf("r" to 5L))
                
    }
}
