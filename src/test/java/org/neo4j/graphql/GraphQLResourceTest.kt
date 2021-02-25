package org.neo4j.graphql

import org.assertj.core.api.Assertions
import org.junit.Before
import org.junit.Test
import org.neo4j.test.server.HTTP
import org.skyscreamer.jsonassert.JSONAssert
import org.skyscreamer.jsonassert.JSONCompareMode

class GraphQLResourceTest : BaseTest() {

    @Before
    fun setup() {
        createSchema()
    }

    @Test
    fun options() {
        val response = HTTP.request("OPTIONS", serverURI.toString(), null)
        Assertions.assertThat(response).extracting { it.status() }.isEqualTo(200)
    }

    @Test
    fun quotedVariables() {
        val response = HTTP.POST(serverURI.toString(), mapOf(
                "query" to "query AllPeopleQuery { Person {name,born} }",
                "variables" to "\"{}\""
        ))
        Assertions.assertThat(response).extracting { it.status() }.isEqualTo(200)
    }

    @Test
    fun quotedVariablesContent() {
        val response = HTTP.POST(serverURI.toString(), mapOf(
                "query" to "query AllPeopleQuery { Person {name,born} }",
                "variables" to "\"{\"answer\":42}\""
        ))
        Assertions.assertThat(response).extracting { it.status() }.isEqualTo(200)
    }

    @Test
    fun testQuery() {
        val response = HTTP.POST(serverURI.toString(), mapOf(
                "query" to "query AllPeopleQuery { Person {name,born} }"
        ))
        Assertions.assertThat(response).extracting { it.status() }.isEqualTo(200)
        JSONAssert.assertEquals(
                "{\"errors\":[],\"data\":[{\"Person\":[{\"born\":1958,\"name\":\"Kevin Bacon\"},{\"born\":1961,\"name\":\"Meg Ryan\"}]}],\"extensions\":null,\"dataPresent\":true}",
                response.rawContent(),
                JSONCompareMode.STRICT
        )
    }

    @Test
    fun testVariables() {
        val response = HTTP.POST(serverURI.toString(),
                mapOf(
                        "query" to "query AllPeopleQuery(\$name:String!) { Person(name:\$name) {name,born} }",
                        "variables" to mapOf("name" to "Meg Ryan")
                ))
        Assertions.assertThat(response).extracting { it.status() }.isEqualTo(200)
        JSONAssert.assertEquals(
                "{\"errors\":[],\"data\":[{\"Person\":[{\"born\":1961,\"name\":\"Meg Ryan\"}]}],\"extensions\":null,\"dataPresent\":true}",
                response.rawContent(),
                JSONCompareMode.STRICT
        )
    }
}
