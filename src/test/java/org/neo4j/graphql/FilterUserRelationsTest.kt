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
class FilterUserRelationsTest {
    private lateinit var db: GraphDatabaseService
    private lateinit var ctx: GraphQLContext
    private var graphQL: GraphQL? = null


    @Before
    @Throws(Exception::class)
    fun setUp() {
        db = TestGraphDatabaseFactory().newImpermanentDatabase()
        (db as GraphDatabaseAPI).dependencyResolver.resolveDependency(Procedures::class.java).let {
            it.registerFunction(GraphQLProcedure::class.java)
            it.registerProcedure(GraphQLProcedure::class.java)
        }
        db.execute("CREATE (saj:User {username:'Saj'}),(koi:User {username:'Koi'}),(thing:Like {username:'thing'}),(saj)-[:LIKES]->(thing)<-[:LIKES]-(koi), (koi)<-[:FOLLOWS]-(saj),(saj)<-[:FOLLOWS]-(saj)")?.close()

        ctx = GraphQLContext(db)
        GraphSchemaScanner.storeIdl(db, schema)
        graphQL = GraphSchema.getGraphQL(db)
    }

    val schema = """
type Like @model {
   user: User @relation (name:"LIKES", direction:IN)
}
type User @model{
   followers: [User] @relation (name:"FOLLOWS", direction: IN)
   following: [User] @relation (name:"FOLLOWS", direction: OUT)
   username: String!
   likes: [Like] @relation (name:"LIKES", direction: OUT)
}
"""

    @After
    @Throws(Exception::class)
    fun tearDown() {
        db.shutdown()
    }

    private fun assertResult(query: String, expected: Any, params: Map<String,Any> = emptyMap()) {
        val ctx2 = GraphQLContext(ctx.db, ctx.log, params)
        val result = graphQL!!.execute(query, ctx2, params)
        if (result.errors.isNotEmpty()) println(result.errors)
        assertEquals(expected, result.getData())
    }

    @Test
    fun singleRelation() {
        val query = """
query{  u: User(username:"Saj"){
    likes(filter:{user:{followers_some:{username:"Saj"}}}){
        user{
          username
        }
      }
  }
}
        """
        // ATTN: Saj has to follow all likers even herself !
        assertResult(query, mapOf("u" to listOf(mapOf("likes" to listOf(mapOf("user" to mapOf("username" to "Koi")))))))
    }
    @Test
    fun userName() {
        val query = """
query{  u: User(username:"Saj"){
    username
  }
}
        """
        assertResult(query, mapOf("u" to listOf(mapOf("username" to "Saj"))))
    }

    @Test
    fun likesUser() {
        val query = """
query{  u: User(username:"Saj"){
        likes {
        user {
          username
        }
      }

  }
}
        """
        assertResult(query, mapOf("u" to listOf(mapOf("likes" to listOf(mapOf("user" to mapOf("username" to "Koi")))))))
    }

}
