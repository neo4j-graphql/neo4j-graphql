package org.neo4j.graphql;

import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.schema.GraphQLSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 24.10.16
 */
public class MetaDataTest {

    private GraphDatabaseService db;
    private GraphQL graphql;

    @Before
    public void setUp() throws Exception {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        db.execute("UNWIND range(1,5) as id CREATE (:User:Person {name:'John '+id, id:id, age:id})").close();
        GraphQLSchema graphQLSchema = MetaData.buildSchema(db);
        graphql = new GraphQL(graphQLSchema);
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }

    @Test
    public void allUsersQuery() throws Exception {
        Map<String, List<Map>> result = executeQuery("query UserQuery { User {id,name,age} User {age,name}}", map());
        assertEquals(2*5, result.get("User").size());
    }
    @Test
    public void oneUserParameterQuery() throws Exception {
        Map<String, List<Map>> result = executeQuery("query UserQuery($name: String!) { User(name:$name) {id,name,age} }", map("name", "John 2"));
        List<Map> users = result.get("User");
        assertEquals(1, users.size());
        assertEquals("John 2", users.get(0).get("name"));
        assertEquals(2L, users.get(0).get("id"));
    }
    @Test
    public void oneUserQuery() throws Exception {
        Map<String, List<Map>> result = executeQuery("query UserQuery { User(id:3) {id,name,age} }", map());
        List<Map> users = result.get("User");
        assertEquals(1, users.size());
        assertEquals("John 3", users.get(0).get("name"));
        assertEquals(3L, users.get(0).get("id"));
    }

    private Map<String,List<Map>> executeQuery(String query, Map<String, Object> arguments) {
        System.out.println("query = " + query);
        ExecutionResult result = graphql.execute(query, db, arguments);
        Object data = result.getData();
        System.out.println("data = " + data);
        List<GraphQLError> errors = result.getErrors();
        System.out.println("errors = " + errors);
        return (Map<String,List<Map>>) result.getData();
    }

}
