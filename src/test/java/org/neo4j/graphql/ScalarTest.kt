package org.neo4j.graphql

import org.junit.After
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.neo4j.graphql.TestUtil.assertResult
import org.neo4j.graphql.TestUtil.execute

class ScalarTest {

    @Before
    @Throws(Exception::class)
    fun setUp() {
        TestUtil.setup(schema)
    }

    val schema = """
# scalar Date
type Movie {
  title: ID!
  released: Int #_Neo4jDateTime
}
type Actor  {
  name: ID!
  born: Long
}
"""

    @After
    @Throws(Exception::class)
    fun tearDown() {
        TestUtil.tearDown()
    }

    @Test
    fun createMovie() {
        assertResult("""mutation { m: createMovie(title:"Forrest Gump", released:1994)  { title } } """, mapOf("m" to mapOf("title" to "Forrest Gump")))
    }

    @Test
    fun updateMovie() {
        createMovieData()
        assertResult("""mutation { m: updateMovie(title:"Forrest Gump", released:1995) { released } }""",
                        mapOf("m" to mapOf("released" to 1995L)))
    }

    @Test
    @Ignore("broken")
    fun updateMovieNoProperty() {
        createMovieData()
        assertResult("""mutation { m: updateMovie(title:"Forrest Gump") { released }}""",
        mapOf("m" to mapOf("released" to 1994L)))
    }
    @Test
    fun updateMovieNullProperty() {
        createMovieData()
        assertResult("""mutation { m: updateMovie(title:"Forrest Gump", released:null) { released }}""",
        mapOf("m" to
                mapOf("released" to null)))
    }

    @Test
    fun findMovie() {
        createMovieData()
            assertResult("""{ movie(title:"Forrest Gump") { title, released } }""",
        mapOf("movie" to listOf(mapOf("title" to "Forrest Gump", "released" to 1994L))))
        assertResult("""{ movie(released:1994) { title, released } }""",
        mapOf("movie" to listOf(mapOf("title" to "Forrest Gump", "released" to 1994L))))
    }
    @Test
    fun findActor() {
        execute("CREATE (:Actor {name:'Keanu Reeves', born:1994})")
        assertResult("""{ actor(name:"Keanu Reeves") { name, born } }""",
        mapOf("actor" to listOf(mapOf("name" to "Keanu Reeves", "born" to 1994L))))
    }

    @Test
    fun findMovieFilter() {
        createMovieData()
        assertResult("""{ movie(filter:{released_gte:1994}) { title, released } }""",
        mapOf("movie" to listOf(mapOf("title" to "Forrest Gump", "released" to 1994L))))
    }
    private fun createMovieData() {
        execute("CREATE (:Movie {title:'Forrest Gump', released:1994})")
    }
}
