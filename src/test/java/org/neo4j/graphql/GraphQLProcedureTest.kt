package org.neo4j.graphql

import org.assertj.core.api.Assertions
import org.assertj.core.groups.Tuple
import org.json.JSONObject
import org.junit.Test
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.graphdb.Relationship
import org.neo4j.internal.helpers.collection.MapUtil
import org.skyscreamer.jsonassert.JSONAssert
import org.skyscreamer.jsonassert.JSONCompareMode

class GraphQLProcedureTest : BaseTest() {
    @Test
    fun testProcedureCall() {
        createSchema()
        val query = "query AllPeopleQuery { Person(born:1961) {name,born} }"
        neo4j.defaultDatabaseService()
                .beginTx()
                .execute("CALL graphql.query(\$query)", mapOf("query" to query))
                .use { result ->
                    Assertions.assertThat(result).hasNext()
                    val row = result.next()
                    JSONAssert.assertEquals(
                            "{\"result\":[{\"Person\":[{\"born\":1961,\"name\":\"Meg Ryan\"}]}]}",
                            JSONObject(row),
                            JSONCompareMode.STRICT
                    )
                    Assertions.assertThat(result).isExhausted
                }
    }

    @Test
    fun testSchemaProcedure() {
        createSchema()
        neo4j.defaultDatabaseService()
                .beginTx()
                .execute("CALL graphql.schema()").use { result ->
                    Assertions.assertThat(result).hasNext()
                    val row = result.next()
                    println("row = $row")
                    @Suppress("UNCHECKED_CAST")
                    Assertions.assertThat(row!!["nodes"] as MutableList<Node>)
                            .extracting<Tuple?, RuntimeException?> { o: Node ->
                                Tuple(
                                        o.labels.iterator().next().name(),
                                        o.allProperties
                                )
                            }
                            .containsExactlyInAnyOrder(
                                    Tuple("Movie", MapUtil.stringMap(
                                            "__name", "Movie",
                                            "title", "String",
                                            "released", "Int",
                                            "tagline", "String"
                                    )),
                                    Tuple("Person", MapUtil.stringMap(
                                            "__name", "Person",
                                            "born", "Int",
                                            "name", "String"
                                    ))
                            )
                    @Suppress("UNCHECKED_CAST")
                    Assertions.assertThat(row["rels"] as MutableList<Relationship>)
                            .extracting<Tuple?, RuntimeException?> { o: Relationship ->
                                Tuple(
                                        o.startNode.labels.iterator().next().name(),
                                        o.endNode.labels.iterator().next().name(),
                                        o.type.name(),
                                        o.getProperty("type"),
                                        o.getProperty("multi"))
                            }
                            .containsExactlyInAnyOrder(
                                    Tuple("Person", "Movie", "movies", "ACTED_IN", true),
                                    Tuple("Movie", "Person", "actors", "ACTED_IN", true)
                            )
                    Assertions.assertThat(result).isExhausted
                }
    }

    @Test
    fun testProcedureCallFail() {
        createSchema()
        try {
            neo4j.defaultDatabaseService()
                    .beginTx()
                    .execute("CALL graphql.execute('foo')")
                    .use { Assertions.fail<Any?>("Procedure call should fail") }
        } catch (e: Exception) {
            Assertions.assertThat(e).isInstanceOf(QueryExecutionException::class.java)
        }
    }

    @Test
    fun testStoreIdl() {
        neo4j.defaultDatabaseService().beginTx().use { tx ->
            try {
                tx.execute("CALL graphql.idl('type Person {name:String}')")
                        .columnAs<String?>("value")
                        .use { Assertions.assertThat(it.next()).isEqualTo("type Person {name:String}") }
                tx.execute("CALL graphql.reset()").use { }
                tx.execute("CALL graphql.idl('type Person {name:String}')")
                        .columnAs<String?>("value")
                        .use { Assertions.assertThat(it.next()).isEqualTo("type Person {name:String}") }
            } finally {
                tx.execute("CALL graphql.idl(null)")
                        .columnAs<String?>("value")
                        .use { Assertions.assertThat(it.next()).isEqualTo("Removed stored GraphQL Schema") }
            }
        }
    }

    @Test
    fun testStoreIdlFail() {
        neo4j.defaultDatabaseService().beginTx().use { tx ->
            try {
                tx.execute("CALL graphql.idl('type Foo {')")
                        .columnAs<String?>("value")
                        .use { Assertions.fail<Any?>("Incorrect schema should fail") }
            } catch (e: RuntimeException) {
                Assertions.assertThat(e)
                        .hasMessageContaining("InvalidSyntaxError{ message=Invalid Syntax : offending token '<EOF>' at line 1 column 11 ,offendingToken=<EOF> ,locations=[SourceLocation{line=1, column=11}")
            }
        }
    }

    @Test
    fun testGetIdl() {
        createSchema()
        neo4j.defaultDatabaseService().beginTx().use { tx ->
            val idl = tx.execute("return graphql.getIdl() as schema")
                    .columnAs<String>("schema").next()
            Assertions.assertThat(idl).isEqualTo(IDL)
        }
    }

    @Test
    fun testGetAugmentedSchema() {
        createSchema()
        neo4j.defaultDatabaseService().beginTx().use { tx ->
            var idl = tx.execute("return graphql.getAugmentedSchema() as schema")
                    .columnAs<String>("schema").next()
            // remove comments
            idl = idl.replace("(?m)^\\s*#.+\\n".toRegex(), "")
            println("idl = $idl")
            Assertions.assertThat(idl)
                    .contains("schema {\n" +
                            "  query: Query\n" +
                            "  mutation: Mutation\n" +
                            "}")
                    .contains("type Movie {")
                    .contains("type Mutation {\n" +
                            "  createMovie(released: Int, tagline: String, title: String!): Movie!\n" +
                            "  createPerson(born: Int, name: String!): Person!\n" +
                            "}")
                    .hasSizeGreaterThan(IDL.length)
        }
    }
}
