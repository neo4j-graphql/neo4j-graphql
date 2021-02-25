package org.neo4j.graphql

import io.netty.handler.codec.http.HttpHeaderNames
import org.assertj.core.api.Assertions
import org.junit.BeforeClass
import org.junit.Test
import org.neo4j.test.server.HTTP
import java.net.URI
import java.net.URL
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse


class GraphQLSchemaResourceTest : BaseTest() {
    @Test
    fun createAndQueryAndDeleteIdl() {
        val idl = "type Person {name:String}"

        // create schema
        var response = sendGraphQl(idl)
        val augmentedSchema = response.body()
        assertResponse(response, 200, "application/graphql")

        // get idl
        response = getResource("idl")
        assertResponse(response, 200, "application/graphql")
        Assertions.assertThat(response.body()).isEqualTo(idl)

        // get augmented
        response = getResource("idl/augmented")
        assertResponse(response, 200, "application/graphql")
        Assertions.assertThat(response.body()).isEqualTo(augmentedSchema)
        deleteSchema(204)
    }

    @Test
    fun testCreateSchemaViaGraphQL() {
        val idl = "type Person {name:String}"
        val response = HTTP.POST(resourceUri.toString(), mapOf("query" to idl))
        Assertions.assertThat(response).extracting { it.status() }.isEqualTo(200)
    }

    @Test
    fun getSchemaNotFound() {
        val response = getResource("idl")
        Assertions.assertThat(response).extracting { it.statusCode() }.isEqualTo(404)
    }

    @Test
    fun getAugmentedSchemaNotFound() {
        val response = getResource("idl/augmented")
        Assertions.assertThat(response).extracting { it.statusCode() }.isEqualTo(404)
    }

    @Test
    fun createInvalidSchema() {
        val idl = "type Person {name:String, failure: Missing }"
        val response = sendGraphQl(idl)
        assertResponse(response, 400, "application/json")
        val error = response.body()
        Assertions.assertThat(error).contains("The field type 'Missing' is not present when resolving type 'Person'")
    }

    @Test
    fun deleteSchemaNotFound() {
        deleteSchema(404)
    }

    private fun deleteSchema(statusCode: Int?) {
        val response = HTTP.withBaseUri(resourceUri).DELETE("")
        if (statusCode != null) {
            Assertions.assertThat(response).extracting { it.status() }.isEqualTo(statusCode)
        }
    }

    private fun assertResponse(response: HttpResponse<*>, statusCode: Int, contentType: String) {
        Assertions.assertThat(response).extracting { it.statusCode() }.isEqualTo(statusCode)
        Assertions.assertThat(response)
            .extracting { it.headers() }
            .extracting { it.firstValue(HttpHeaderNames.CONTENT_TYPE.toString()).orElse(null) }
            .isEqualTo(contentType)
    }

    fun sendGraphQl(idl: String): HttpResponse<String> {
        val request = HttpRequest.newBuilder(resourceUri)
            .POST(HttpRequest.BodyPublishers.ofString(idl))
            .setHeader("Content-Type", "application/graphql")
            .build()
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    }

    fun getResource(path: String): HttpResponse<String> {
        val request = HttpRequest.newBuilder(resourceUri.resolve(path)).GET().build()
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    }

    companion object {
        private lateinit var resourceUri: URI
        private lateinit var httpClient: HttpClient

        @BeforeClass
        @JvmStatic
        fun setUpResource() {
            resourceUri = URL(
                neo4j.httpURI().toURL(),
                "graphql/" + neo4j.defaultDatabaseService().databaseName() + "/idl"
            ).toURI()
            httpClient = HTTP.newClient()
        }
    }
}
