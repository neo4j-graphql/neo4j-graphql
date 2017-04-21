package org.neo4j.graphql;

import java.net.URL;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilders;
import org.neo4j.test.server.HTTP;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 24.10.16
 */
public class GraphQLResourceEndToEndTest
{

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
    public void testPostIdlAndQueryAgainstIt() throws Exception {
        String idlEndpoint = new URL( serverURI, "idl" ).toString();

        String schema = "type Person {  name: String! born: Int movies: [Movie] @out(name:\"ACTED_IN\") }" +
                        "type Movie  {  title: String! released: Int tagline: String actors: [Person] @in(name:\"ACTED_IN\") }";

        HTTP.Response schemaResponse = HTTP.POST( idlEndpoint, HTTP.RawPayload.rawPayload(schema));
        assertEquals(200, schemaResponse.status());

        String mutation = "mutation { ";
        mutation += "createPerson(name: \"Kevin Bacon\" born: 1958 ) ";
        mutation += "createMovie(title: \"Apollo 13\" released: 1995 ) ";
        mutation += "addPersonMovies(name:\"Kevin Bacon\", movies:[\"Apollo 13\", \"The Matrix\"])";
        mutation += "}";

        HTTP.Response mutationResponse = HTTP.POST(serverURI.toString(), map("query", mutation));
        System.out.println( "mutationResponse = " + mutationResponse.rawContent() );
        assertEquals(200, mutationResponse.status());


        String query = "query { Person(name: \"Kevin Bacon\") { born, movies { title released tagline actors { name }  } } }";
        HTTP.Response queryResponse = HTTP.POST(serverURI.toString(), map("query", query));

        Map<String, Map<String,List<Map>>> result = queryResponse.content();

        System.out.println( "result = " + result );

        List<Map> data = result.get("data").get("Person");

        assertEquals(1, data.size());
        Map kevinBacon = data.get( 0 );
        assertEquals(1958, kevinBacon.get( "born" ));

        Map apollo13 = ((List<Map>) kevinBacon.get( "movies" )).get( 0 );
        assertEquals("Apollo 13", apollo13.get( "title" ) );
        assertEquals(1995, apollo13.get( "released" ) );

        assertEquals("Kevin Bacon", ((List<Map>)apollo13.get( "actors" )).get( 0 ).get( "name" ));

    }
}
