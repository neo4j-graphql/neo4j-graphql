package org.neo4j.graphql;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.test.server.HTTP;

import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 24.10.16
 */
public class GraphQLResourceTest {

    private static ServerControls neo4j;
    private static URL serverURI;

    @BeforeClass
    public static void setUp() throws Exception {
        neo4j = TestServerBuilders
                .newInProcessBuilder()
                .withExtension("/graphql", GraphQLResource.class)
                .withProcedure(GraphQLProcedure.class)
                .withFunction(GraphQLProcedure.class)
                .withFixture("CREATE (:Person {name:'Kevin Bacon',born:1958})-[:ACTED_IN]->(:Movie {title:'Apollo 13',released:1995}),(:Person {name:'Meg Ryan',born:1961})")
                .newServer();
        serverURI = new URL(neo4j.httpURI().toURL(), "graphql/");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        neo4j.close();
    }

    @Test
    public void options() throws Exception {
        HTTP.Response response = HTTP.request("OPTIONS",serverURI.toString(), null);

        assertEquals(200, response.status());
    }

    @Test
    public void quotedVariables() throws Exception {
        HTTP.Response response = HTTP.POST(serverURI.toString(), map("query", "query AllPeopleQuery { Person {name,born} }","variables","\"{}\""));

        assertEquals(200, response.status());
    }
    @Test
    public void quotedVariablesContent() throws Exception {
        HTTP.Response response = HTTP.POST(serverURI.toString(), map("query", "query AllPeopleQuery { Person {name,born} }","variables","\"{\"answer\":42}\""));

        assertEquals(200, response.status());
    }

    @Test
    public void allPeople() throws Exception {
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
        HTTP.Response response = HTTP.POST(serverURI.toString(), map("query", "query AllPeopleQuery { Person(born:1961) {name,born} }","variables","null"));

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
        HTTP.Response response = HTTP.POST(serverURI.toString(), map("query", "query AllPeopleQuery($name:String!) { Person(name:$name) {name,born} }","variables",map("name","Meg Ryan")));

        assertEquals(200, response.status());

        Map<String, Map<String,List<Map>>> result = response.content();

        System.out.println("result = " + result);
        assertNull(result.get("errors"));
        List<Map> data = result.get("data").get("Person");
        assertEquals(1,data.size());
        assertEquals("Meg Ryan",data.get(0).get("name"));
    }

    @Test
    public void testProcedureCall() throws Exception {
        String query = "query AllPeopleQuery { Person(born:1961) {name,born} }";
        GraphDatabaseService db = neo4j.graph();
        Result result = db.execute("CALL graphql.execute({query},{})",map("query",query));
        assertEquals(true,result.hasNext());
        Map<String, Object> row = result.next();
        System.out.println("row = " + row);
        List<Map>  data = (List<Map>) ((Map) row.get("result")).get("Person");
        assertEquals(1,data.size());
        assertEquals("Meg Ryan",data.get(0).get("name"));
        assertEquals(false,result.hasNext());
        result.close();
    }


    @Test
    public void testCypherProcedure() throws Exception {
        testCypherCall("CALL graphql.run({query},{born:{born}},true)");
    }

    @Test
    public void testCypherFunction() throws Exception {
        testCypherCall("RETURN graphql.run({query},{born:{born}},false) as row");
    }
    @Test
    public void testComplexProcedureCall() throws Exception {
        String call = "CALL graphql.queryForNodes(\"MATCH (p:Person {born:1961}) RETURN p\") YIELD node AS `Person`\n" +
                "RETURN labels(`Person`) AS `_labels`,\n" +
                "`Person` {.`name`, .`born`} AS `row`";
        testCypherCall(call);
    }

    private void testCypherCall(String call) {
        String query = "MATCH (p:Person {born:{born}}) RETURN p {.name,.born}";
        GraphDatabaseService db = neo4j.graph();
        Result result = db.execute(call,map("query",query,"born",1961));
        assertEquals(true,result.hasNext());
        Map<String, Object> row = result.next();
        System.out.println("row = " + row);
        Map  data = (Map)row.get("row");
        assertEquals("Meg Ryan",data.get("name"));
        assertEquals(false,result.hasNext());
        result.close();
    }

    @Test
    public void testSchemaProcedure() throws Exception {
        GraphDatabaseService db = neo4j.graph();
        Result result = db.execute("CALL graphql.schema()");
        assertEquals(true,result.hasNext());
        Map<String, Object> row = result.next();
//        System.out.println("row = " + row);
        List<Node> nodes = (List<Node>)row.get("nodes");
        assertEquals(2,nodes.size());
        Node person = nodes.get(0);
        assertEquals("Person", person.getLabels().iterator().next().name());
        assertEquals("Person", person.getProperty("name"));
        assertEquals("String", person.getProperty(" name"));
        assertEquals("Long", person.getProperty(" born"));
        List<Relationship> rels = (List<Relationship>)row.get("rels");
        assertEquals(2, rels.size());
        Relationship actedIn = rels.get(0);
        assertEquals("Person", actedIn.getStartNode().getLabels().iterator().next().name());
        assertEquals("Movie", actedIn.getEndNode().getLabels().iterator().next().name());
        assertEquals("actedIn", actedIn.getType().name());
        assertEquals("ACTED_IN", actedIn.getProperty("type"));
        assertEquals(false, actedIn.getProperty("multi"));

        actedIn = rels.get(1);
        assertEquals("Movie", actedIn.getStartNode().getLabels().iterator().next().name());
        assertEquals("Person", actedIn.getEndNode().getLabels().iterator().next().name());
        assertEquals("actedIn", actedIn.getType().name());
        assertEquals("ACTED_IN", actedIn.getProperty("type"));
        assertEquals(false, actedIn.getProperty("multi"));

        assertEquals(false,result.hasNext());
        result.close();
    }

    @Test
    public void testProcedureCallFail() throws Exception {
        try {
            GraphDatabaseService db = neo4j.graph();
            Result result = db.execute("CALL graphql.execute('foo',{})");
            fail("Procedure call should fail");
        } catch(Exception e) {
            assertEquals(true,e.getMessage().contains("InvalidSyntaxError"));
        }
    }

    @Test
    public void testStoreIdl() throws Exception {
        GraphDatabaseService db = neo4j.graph();

        try (ResourceIterator<String> it = db.execute("CALL graphql.idl('type Person')").columnAs("value")){
            fail("Incorrect schema should fail");
        } catch(RuntimeException e) {
            assertEquals(true, e.getMessage().contains("Error parsing IDL expected '{' got '<EOF>' line 1 column 11"));
        }
        try (ResourceIterator<String> it = db.execute("CALL graphql.idl('type Person {name:String}')").columnAs("value")) {
            assertEquals(true, it.next().startsWith("{Person=MetaData{type='Person', properties={name=PropertyInfo(fieldName=name, type=String"));
        }
        try (ResourceIterator<String> it = db.execute("CALL graphql.idl(null)").columnAs("value")) {
            assertEquals("Removed stored GraphQL Schema",it.next());
        }
    }

    @Test
    @Ignore
    public void testPostIdl() throws Exception {
        Map<String, Object> result = postIdl("type Person", 500);
        Object errors = result.get("error");
        assertNotNull(errors);
        assertTrue(errors.toString().contains("Error parsing IDL expected '{' got '<EOF>' line 1 column 11"));

        Map<String, Object> result2 = postIdl("type Person {name:String}", 200);
        assertNull(result2.get("error"));
        Map data = (Map) result2.get("Person");
        System.out.println("data = " + data);
        assertEquals("Person",data.get("type"));
    }

    @Test
    @Ignore
    public void testPostIdlJson() throws Exception {
        Map<String, Object> result = postIdl(map("query","type Person"), 500);
        Object errors = result.get("error");
        assertNotNull(errors);
        assertTrue(errors.toString().contains("Error parsing IDL expected '{' got '<EOF>' line 1 column 11"));

        Map<String, Object> result2 = postIdl(map("query","type Person {name:String}"), 200);
        assertNull(result2.get("error"));
        Map data = (Map) result2.get("Person");
        System.out.println("data = " + data);
        assertEquals("Person",data.get("type"));
    }

    private Map<String, Object> postIdl(Object payload, int status) {
        HTTP.Response response = HTTP.POST(serverURI.toString()+"idl/", payload);
        assertEquals(status, response.status());
        Map<String, Object> result = response.content();
        System.out.println("result = " + result);
        return result;
    }
}
