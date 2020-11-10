package org.neo4j.graphql

import org.assertj.core.api.Assertions
import org.junit.After
import org.junit.AfterClass
import org.junit.BeforeClass
import org.neo4j.harness.Neo4j
import org.neo4j.harness.Neo4jBuilders
import java.net.URL

open class BaseTest {
    @After
    fun cleanUp() {
        neo4j.defaultDatabaseService().beginTx().use { tx ->
            tx.execute("CALL graphql.reset()").close()
        }
    }

    protected fun createSchema(idl: String = IDL) {
        neo4j.defaultDatabaseService().beginTx().use { tx ->
            tx.execute("CALL graphql.idl(\$idl)", mapOf("idl" to idl))
                    .columnAs<String?>("value")
                    .use { it -> Assertions.assertThat(it.next()).isEqualTo(idl) }
        }
    }

    companion object {
        lateinit var neo4j: Neo4j
        lateinit var serverURI: URL
        const val IDL: String = """
            type Movie  {
                title: String!
                released: Int
                tagline: String
                actors: [Person] @relation(name:"ACTED_IN",direction:IN)
            }
            type Person {
                name: String!
                        born: Int
                movies: [Movie] @relation(name:"ACTED_IN")
            }
            """

        @BeforeClass
        @JvmStatic
        fun setUp() {
            neo4j = Neo4jBuilders
                    .newInProcessBuilder()
                    .withUnmanagedExtension("/graphql", "org.neo4j.graphql")
                    .withProcedure(GraphQLProcedure::class.java)
                    .withFunction(GraphQLProcedure::class.java)
                    .withProcedure(apoc.cypher.Cypher::class.java)
                    .withFunction(apoc.cypher.CypherFunctions::class.java)
                    .withFixture("CREATE " +
                            "(:Person {name:'Kevin Bacon',born:1958})-[:ACTED_IN]->(:Movie {title:'Apollo 13',released:1995})," +
                            "(:Person {name:'Meg Ryan',born:1961})")
                    .build()
            serverURI = URL(neo4j.httpURI().toURL(), "graphql/" + neo4j.defaultDatabaseService().databaseName())
        }

        @AfterClass
        @JvmStatic
        fun tearDown() {
            neo4j.close()
        }
    }
}
