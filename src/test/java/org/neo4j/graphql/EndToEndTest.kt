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
        interface Person {
            name: ID!
            born: Int
            movies: [Movie]
            score(value:Int!): Int @cypher(statement:"RETURN {value}")
        }

        type Actor implements Person {
            name: ID!
            born: Int
            movies: [Movie] @relation(name:"ACTED_IN")
            score(value:Int!): Int @cypher(statement:"RETURN {value}")

            totalMoviesCount: Int @cypher(statement: "MATCH (this)-[:ACTED_IN]->() RETURN count(*) AS totalMoviesCount")
            recommendedColleagues: [Person] @cypher(statement: "WITH {this} AS this MATCH (this)-[:ACTED_IN]->()<-[:ACTED_IN]-(other) RETURN other")
            namedColleagues(name: String!): [Person] @cypher(statement: "WITH {this} AS this MATCH (this)-[:ACTED_IN]->()<-[:ACTED_IN]-(other) WHERE other.name CONTAINS {name} RETURN other")
        }

        type Director implements Person {
            name: ID!
            born: Int
            movies: [Movie] @relation(name:"DIRECTED")
            score(value:Int!): Int @cypher(statement:"RETURN {value}")
        }

        type Movie  {
            title: ID!
            released: Int
            tagline: String
            genre: Genre
            actors: [Actor] @relation(name:"ACTED_IN",direction:"IN")
            directors: [Director] @relation(name:"DIRECTED",direction:"IN")
         }

         schema {
            mutation: MutationType
            query: QueryType
         }
         type MutationType {
            newPerson(person:PersonInput) : String @cypher(statement:"CREATE (:Person {name:({person}).name,born:({person}).born})")
            newMovie(title:ID!, released:Int, tagline:String) : Movie @cypher(statement:"MERGE (m:Movie {title:{title}}) ON CREATE SET m += {released:{released}, tagline:{tagline}} RETURN m")
         }
         type QueryType {
            personByName(name:ID!) : Person @cypher(statement:"MATCH (p:Person {name:{name}}) RETURN p")
            personByBorn(born:Int!) : [Person] @cypher(statement:"MATCH (p:Person {born:{born}}) RETURN p")
            movieCount : Int @cypher(statement:"MATCH (:Movie) RETURN count(*)")
            movieByGenre(genre: Genre) : [Movie] @cypher(statement:"MATCH (m:Movie {genre:{genre}}) RETURN m")
            movieGenre(title: String) : Genre @cypher(statement:"MATCH (m:Movie {title:{title}}) RETURN m.genre")
         }
         enum Genre {
            Action, Drama, Family, Horror, SciFi
         }
         input PersonInput {
            name: ID!
            born: Int
         }
         """

        val schemaResponse = HTTP.POST(idlEndpoint, HTTP.RawPayload.rawPayload(schema))
        assertEquals(200, schemaResponse.status().toLong())

        val mutation = """
        mutation {
            kb: createActor(name: "Kevin Bacon" born: 1958 )
            mr: createActor(name: "Meg Ryan" born: 1961 )
            a13: createMovie(title: "Apollo 13" released: 1995 tagline: "...", genre: SciFi )
            matrix: createMovie(title: "The Matrix" released: 2001 tagline: "There is no spoon" )

            kb_matrix: addActorMovies(name:"Kevin Bacon" movies:["Apollo 13", "The Matrix"])
            mr_a13: addActorMovies(name:"Meg Ryan" movies:["Apollo 13"])

            th: newPerson(person: {name:"Tom Hanks" born:1950})
            fg: newMovie(title:"Forrest Gump") { title }
        }
        """

        val mutationResponse = HTTP.POST(serverURI!!.toString(), mapOf("query" to mutation))
        println("mutationResponse = " + mutationResponse.rawContent())
        assertEquals(200, mutationResponse.status().toLong())

        val thResult = neo4j!!.graph().execute("MATCH (p:Person {name:'Tom Hanks', born:1950}) RETURN p.name as name").columnAs<String>("name")
        assertEquals(true, thResult.hasNext())
        assertEquals("Tom Hanks", thResult.next())
        assertEquals(false, thResult.hasNext())

        val fgResult = neo4j!!.graph().execute("MATCH (m:Movie {title:'Forrest Gump'}) RETURN m.title as title").columnAs<String>("title")
        assertEquals(true, fgResult.hasNext())
        assertEquals("Forrest Gump", fgResult.next())
        assertEquals(false, fgResult.hasNext())

        val query = """
            query {
                Person(name: "Kevin Bacon") {
                    born

                    ... on Actor {
                        totalMoviesCount
                        recommendedColleagues {
                            ... name
                        }
                        namedColleagues(name: "Meg") {
                            ... name
                            ... on Actor {
                                totalMoviesCount
                            }
                        }
                        movies {
                            title
                            released
                            tagline
                            actors {
                                ... name
                                born
                            }
                         }
                    }

                    score(value:7)

                 }
            }
            fragment name on Actor { name }
        """

        val result = executeQuery(query)

        assertNull(result["errors"])

        val data = result["data"]!!["Person"] as List<Map<String,Any>>

        assertEquals(1, data?.size)

        val kevinBacon = data!!.get(0)
        assertEquals(1958, kevinBacon["born"])
        assertEquals(2, kevinBacon["totalMoviesCount"])

        val namedColleagues = (kevinBacon["namedColleagues"] as List<Map<*, *>>)
        assertEquals(setOf("Meg Ryan"), namedColleagues.map { it["name"] }.toSet())

        assertEquals(7, kevinBacon["score"])

        val movies = (kevinBacon["movies"] as List<Map<*, *>>)
        assertEquals(setOf("Apollo 13","The Matrix"), movies.map { it["title"] }.toSet())
        assertEquals(setOf(1995,2001), movies.map { it["released"] }.toSet())

        val apollo13 = movies.find { it["title"] == "Apollo 13" }!!
        assertEquals(setOf("Kevin Bacon","Meg Ryan"), (apollo13["actors"] as List<Map<*, *>>).map{ it["name"]}.toSet())

        val queryFields = """
        query { personByName(name: "Kevin Bacon") { born } }
        """

        val fieldResult = executeQuery(queryFields)
        println(fieldResult)
        assertNull(fieldResult["errors"])
        assertEquals(mapOf("born" to 1958), fieldResult["data"]!!["personByName"])

        val queryByGenre = """
        query { movieByGenre(genre: SciFi) { title } }
        """
        val queryByGenreResult = executeQuery(queryByGenre)
        println(queryByGenreResult)
        assertNull(queryByGenreResult["errors"])
        val movieByGenre = queryByGenreResult["data"]!!["movieByGenre"] as List<Map<String,Any>>?
        assertEquals(1, movieByGenre?.size)
        assertEquals("Apollo 13", movieByGenre!!.get(0)["title"])

        val queryGenre = """
        query { movieGenre(title:"Apollo 13") }
        """
        val genreResult = executeQuery(queryGenre)
        println(genreResult)
        assertNull(genreResult["errors"])
        assertEquals("SciFi", genreResult["data"]!!["movieGenre"])

        val updateMutation = """
        mutation {
            kb: updateActor(name: "Kevin Bacon" born: 1960 )
            mr: deleteActor(name: "Meg Ryan" )
            kb_update: deleteActorMovies(name:"Kevin Bacon" movies:["The Matrix"])
        }
        """

        val updateMutationResponse = HTTP.POST(serverURI!!.toString(), mapOf("query" to updateMutation))
        println("updateMutationResponse = ${updateMutationResponse}")
        assertEquals(200, mutationResponse.status().toLong())

        val queryResult = neo4j!!.graph().execute("MATCH (p:Actor)-[:ACTED_IN]->(m:Movie) RETURN p.name, p.born, m.title")
        assertTrue(queryResult.hasNext())
        val row = queryResult.next()
        assertEquals("Kevin Bacon",row.get("p.name"))
        assertEquals(1960,row.get("p.born"))
        assertEquals("Apollo 13",row.get("m.title"))
        assertFalse(queryResult.hasNext())
    }


    @Test
    @Throws(Exception::class)
    fun testNestedDynamicFields() {
        val idlEndpoint = URL(serverURI, "idl").toString()

        val schema = """
schema {
  mutation: Mutation
  query: Query
}

type Query {
  ## queriesRootQuery
  getUser(userId: ID): UserData
    @cypher(statement: "MATCH (u:User{id: {userId}})-[:CREATED_MAP]->(m:Map) WITH collect({id: m.id, name: m.name}) AS mapsCreated, u RETURN {firstName: u.firstName, lastName: u.lastName, organization: u.organization, mapsCreated: mapsCreated}", passThrough:true)
}

type Mutation {
  initializeMap(userId: ID, mapId: ID, name: String): ID
    @cypher(statement: "MATCH (u:User{id: {userId}}) CREATE (cm:Map{id: {mapId}, name: {name}}), (u)-[:CREATED_MAP]->(cm) RETURN cm.id")
}

type User {
  id: ID
  userName: String
  firstName: String
  lastName: String
  organization: String
  mapsCreated: [Map] @relation(name:"CREATED_MAP", direction:"OUT")
}

type Map {
  id: ID
  name: String
}

type UserData {
  firstName: String
  lastName: String
  organization: String
  mapsCreated: [MapsCreated]
}

type MapsCreated {
  id: String
  name: String
}
"""

        val schemaResponse = HTTP.POST(idlEndpoint, HTTP.RawPayload.rawPayload(schema))
        assertEquals(200, schemaResponse.status().toLong())


        val mutation = """
        mutation {
            u: createUser(id: "123", userName: "JonDoe", firstName: "Jon", lastName: "Doe", organization: "JD")
            m: initializeMap(userId: "123", mapId: "321", name: "Map321")
        }
        """

        val mutationResponse = HTTP.POST(serverURI!!.toString(), mapOf("query" to mutation))
        println("mutationResponse = " + mutationResponse.rawContent())
        assertEquals(200, mutationResponse.status().toLong())


        val query = """
         query queriesRootQuery {
            user: getUser(userId: "123") {
              firstName lastName organization
              mapsCreated {id}
         } }
        """

        val result = executeQuery(query)

        println(result)

        assertNull(result["errors"])

        val user = result["data"]!!["user"] as Map<String,Any>

        assertEquals("Jon", user["firstName"])
        assertEquals("JD", user["organization"])
        assertEquals(listOf(mapOf("id" to "321")), user["mapsCreated"])
    }




    private fun executeQuery(query: String): Map<String, Map<String, Any>> {
        val queryResponse = HTTP.POST(serverURI!!.toString(), mapOf("query" to query))

        println(ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(queryResponse.content()))

        return queryResponse.content<Map<String, Map<String, List<Map<*, *>>>>>()
    }
}
