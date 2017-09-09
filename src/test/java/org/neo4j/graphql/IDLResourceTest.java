package org.neo4j.graphql;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.test.server.HTTP;

import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 24.10.16
 */
public class IDLResourceTest {

    private static ServerControls neo4j;
    private static URL serverURI;

    @BeforeClass
    public static void setUp() throws Exception {
        neo4j = TestServerBuilders
                .newInProcessBuilder()
                .withExtension("/graphql", GraphQLResource.class)
                .withProcedure(GraphQLProcedure.class)
                .newServer();
        serverURI = new URL(neo4j.httpURI().toURL(), "graphql/");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        neo4j.close();
    }

    @Test
    public void storeAndUseIdl() throws Exception {
        String idlUrl = serverURI.toURI().resolve("idl").toString();
        HTTP.Response response = HTTP.withHeaders("Content-Type","text/plain","Accept","text/plain")
                .POST(idlUrl, HTTP.RawPayload.rawPayload("type Person { name: String, born: Int }"));

        assertEquals(200, response.status());

        String result = response.rawContent();
        System.out.println("result = " + result);
        assertEquals(true, result.contains("\"Person\":{\"type\":\"Person\""));
        assertEquals(true, result.contains("\"fieldName\":\"name\",\"type\":{\"name\":\"String\""));
        assertEquals(true, result.contains("\"fieldName\":\"born\",\"type\":{\"name\":\"Int\""));

        HTTP.Response graphQlResponse = HTTP.POST(serverURI.toString(), map("query", "{ __schema { queryType { fields { name }}}}"));

        assertEquals(200, graphQlResponse.status());

        Map<String, Map<String,List<Map>>> graphQlResult = graphQlResponse.content();
        assertEquals(true, graphQlResult.toString().contains("name=Person"));

        HTTP.Response delete = HTTP.request("DELETE", idlUrl, null);
        assertEquals(200, delete.status());
    }

}
