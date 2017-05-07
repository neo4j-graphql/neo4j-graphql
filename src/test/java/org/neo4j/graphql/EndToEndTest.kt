package org.neo4j.graphql

import junit.framework.Assert.assertEquals
import junit.framework.Assert.assertNull
import org.codehaus.jackson.map.ObjectMapper
import org.junit.*
import org.neo4j.harness.ServerControls
import org.neo4j.harness.TestServerBuilders
import org.neo4j.test.server.HTTP
import java.net.URL
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class EndToEndTest {

    private var neo4j: ServerControls? = null
    private var serverURI: URL? = null

    @Before
    @Throws(Exception::class)
    fun setUp() {
        neo4j = TestServerBuilders
                .newInProcessBuilder()
                .withExtension("/graphql", GraphQLResource::class.java)
                .withProcedure(GraphQLProcedure::class.java)
                .withFunction(GraphQLProcedure::class.java)
                .newServer()
        serverURI = URL(neo4j!!.httpURI().toURL(), "graphql/")
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        neo4j!!.close()
    }

    @Test
    @Throws(Exception::class)
    fun testPostIdlAndQueryAgainstIt() {
        val idlEndpoint = URL(serverURI, "idl").toString()

        val schema = """
        type Person {
            name: ID!
            born: Int
            movies: [Movie] @relation(name:"ACTED_IN")
            totalMoviesCount: Int @cypher(statement: "WITH {this} AS this MATCH (this)-[:ACTED_IN]->() RETURN count(*) AS totalMoviesCount")
            recommendedColleagues: [Person] @cypher(statement: "WITH {this} AS this MATCH (this)-[:ACTED_IN]->()<-[:ACTED_IN]-(other) RETURN other")
            score(value:Int!): Int @cypher(statement:"RETURN {value}")
        }

        type Movie  {
            title: ID!
            released: Int
            tagline: String
            actors: [Person] @relation(name:"ACTED_IN",direction:"IN")
         }
         """

        val schemaResponse = HTTP.POST(idlEndpoint, HTTP.RawPayload.rawPayload(schema))
        assertEquals(200, schemaResponse.status().toLong())

        val mutation = """
        mutation {
            kb: createPerson(name: "Kevin Bacon" born: 1958 )
            mr: createPerson(name: "Meg Ryan" born: 1961 )
            a13: createMovie(title: "Apollo 13" released: 1995 tagline: "..." )
            matrix: createMovie(title: "The Matrix" released: 2001 tagline: "Cypher, not as good as GraphQL" )
            kb_matrix: addPersonMovies(name:"Kevin Bacon" movies:["Apollo 13", "The Matrix"])
            mr_a13: addPersonMovies(name:"Meg Ryan" movies:["Apollo 13"])
        }
        """

        val mutationResponse = HTTP.POST(serverURI!!.toString(), mapOf("query" to mutation))
        println("mutationResponse = " + mutationResponse.rawContent())
        assertEquals(200, mutationResponse.status().toLong())


        val query = """
            query {
                Person(name: "Kevin Bacon") {
                    born,
                    totalMoviesCount
                    recommendedColleagues {
                        name
                    }
                    score(value:7)
                    movies {
                        title
                        released
                        tagline
                        actors {
                            name
                            born
                        }
                     }
                 }
            }
        """

        val queryResponse = HTTP.POST(serverURI!!.toString(), mapOf("query" to query))

        println(ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResponse.content()))

        val result = queryResponse.content<Map<String, Map<String, List<Map<*, *>>>>>()

        assertNull(result["errors"])

        val data = result["data"]!!["Person"]

        assertEquals(1, data?.size)

        val kevinBacon = data!!.get(0)
        assertEquals(1958, kevinBacon["born"])
        assertEquals(7, kevinBacon["score"])
        assertEquals(2, kevinBacon["totalMoviesCount"])

        val movies = (kevinBacon["movies"] as List<Map<*, *>>)
        assertEquals(setOf("Apollo 13","The Matrix"), movies.map { it["title"] }.toSet())
        assertEquals(setOf(1995,2001), movies.map { it["released"] }.toSet())

        val apollo13 = movies.find { it["title"] == "Apollo 13" }!!
        assertEquals(setOf("Kevin Bacon","Meg Ryan"), (apollo13["actors"] as List<Map<*, *>>).map{ it["name"]}.toSet())

        val updateMutation = """
        mutation {
            kb: updatePerson(name: "Kevin Bacon" born: 1960 )
            mr: deletePerson(name: "Meg Ryan" )
            kb_update: deletePersonMovies(name:"Kevin Bacon" movies:["The Matrix"])
        }
        """

        val updateMutationResponse = HTTP.POST(serverURI!!.toString(), mapOf("query" to updateMutation))
        println("updateMutationResponse = ${updateMutationResponse}")
        assertEquals(200, mutationResponse.status().toLong())

        val queryResult = neo4j!!.graph().execute("MATCH (p:Person)-[:ACTED_IN]->(m:Movie) RETURN p.name, p.born, m.title")
        assertTrue(queryResult.hasNext())
        val row = queryResult.next()
        assertEquals("Kevin Bacon",row.get("p.name"))
        assertEquals(1960L,row.get("p.born"))
        assertEquals("Apollo 13",row.get("m.title"))
        assertFalse(queryResult.hasNext())
    }



}
