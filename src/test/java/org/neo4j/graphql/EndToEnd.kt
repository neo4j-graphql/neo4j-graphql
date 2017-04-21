package org.neo4j.graphql

import junit.framework.Assert.assertEquals
import org.junit.*
import org.neo4j.harness.ServerControls
import org.neo4j.harness.TestServerBuilders
import org.neo4j.test.server.HTTP
import java.net.URL

class EndToEnd {

    private var neo4j: ServerControls? = null
    private var serverURI: URL? = null

    @Before
    @Throws(Exception::class)
    fun setUp() {
        neo4j = TestServerBuilders
                .newInProcessBuilder()
                .withExtension("/graphql", GraphQLResource::class.java)
                .withProcedure(GraphQLProcedure::class.java)
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
            name: String!
            born: Int
            movies: [Movie] @out(name:"ACTED_IN")
        }

        type Movie  {
            title: String!
            released: Int
            tagline: String
            actors: [Person] @in(name:"ACTED_IN")
         }
         """

        val schemaResponse = HTTP.POST(idlEndpoint, HTTP.RawPayload.rawPayload(schema))
        assertEquals(200, schemaResponse.status().toLong())

        val mutation = """
        mutation {
            createPerson(name: "Kevin Bacon" born: 1958 )
            createMovie(title: "Apollo 13" released: 1995 )
            addPersonMovies(name:"Kevin Bacon", movies:["Apollo 13", "The Matrix"])
        }
        """

        val mutationResponse = HTTP.POST(serverURI!!.toString(), mapOf("query" to mutation))
        println("mutationResponse = " + mutationResponse.rawContent())
        assertEquals(200, mutationResponse.status().toLong())


        val query = """
            query {
                Person(name: "Kevin Bacon") {
                    born,
                    movies {
                        title
                        released
                        tagline
                        actors {
                            name
                        }
                     }
                 }
            }
        """

        val queryResponse = HTTP.POST(serverURI!!.toString(), mapOf("query" to query))

        val result = queryResponse.content<Map<String, Map<String, List<Map<*, *>>>>>()

        println("result = " + result)

        val data = result["data"]!!["Person"]!!

        assertEquals(1, data.size.toLong())
        val kevinBacon = data.get(0)
        assertEquals(1958, kevinBacon["born"])

        val apollo13 = (kevinBacon["movies"] as List<Map<*, *>>)[0]
        assertEquals("Apollo 13", apollo13["title"])
        assertEquals(1995, apollo13["released"])

        assertEquals("Kevin Bacon", (apollo13["actors"] as List<Map<*, *>>)[0]["name"])

    }



}
