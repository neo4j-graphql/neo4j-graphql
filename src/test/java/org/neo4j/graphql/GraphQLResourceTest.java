package org.neo4j.graphql;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 24.10.16
 */
public class GraphQLResourceTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withExtension("/graphql", GraphQLResource.class)
            .withFixture("CREATE (:Person {name:'Kevin Bacon',born:1958})-[:ACTED_IN]->(:Movie {title:'Apollo 13',released:1995}),(:Person {name:'Meg Ryan',born:1961})");

    @Test
    public void allPeople() throws Exception {
        URL serverURI = new URL(neo4j.httpURI().toURL(), "graphql/");

        HTTP.Response response = HTTP.POST(serverURI.toString(), map("query", "query AllPeopleQuery { Person {name,born} }"));

        assertEquals(200, response.status());

        Map<String, Map<String,List<Map>>> result = response.content();

        System.out.println("result = " + result);
        assertNull(result.get("errors"));
        List<Map> data = result.get("data").get("Person");
        assertEquals(2,data.size());
        assertEquals("Kevin Bacon",data.get(0).get("name"));
        assertEquals(1958,data.get(0).get("born"));
        assertEquals("Meg Ryan",data.get(1).get("name"));
    }

    @Test
    public void personByYear() throws Exception {
        URL serverURI = new URL(neo4j.httpURI().toURL(), "graphql/");

        HTTP.Response response = HTTP.POST(serverURI.toString(), map("query", "query AllPeopleQuery { Person(born:1961) {name,born} }"));

        assertEquals(200, response.status());

        Map<String, Map<String,List<Map>>> result = response.content();

        System.out.println("result = " + result);
        assertNull(result.get("errors"));
        List<Map> data = result.get("data").get("Person");
        assertEquals(1,data.size());
        assertEquals("Meg Ryan",data.get(0).get("name"));
    }
    @Test
    public void personByNameParameter() throws Exception {
        URL serverURI = new URL(neo4j.httpURI().toURL(), "graphql/");

        HTTP.Response response = HTTP.POST(serverURI.toString(), map("query", "query AllPeopleQuery($name:String!) { Person(name:$name) {name,born} }","variables",map("name","Meg Ryan")));

        assertEquals(200, response.status());

        Map<String, Map<String,List<Map>>> result = response.content();

        System.out.println("result = " + result);
        assertNull(result.get("errors"));
        List<Map> data = result.get("data").get("Person");
        assertEquals(1,data.size());
        assertEquals("Meg Ryan",data.get(0).get("name"));
    }
}
