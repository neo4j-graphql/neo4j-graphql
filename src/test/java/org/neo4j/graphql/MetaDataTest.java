package org.neo4j.graphql;

import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.schema.GraphQLSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;
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
        db.execute("CREATE (earth:Location {name:'Earth'}) WITH earth UNWIND range(1,5) as id CREATE (:User:Person {name:'John '+id, id:id, age:id})-[:LIVES_ON]->(earth)").close();
        GraphQLSchema graphQLSchema = MetaData.buildSchema(db);
        graphql = new GraphQL(graphQLSchema);
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }

    @Test
    public void sampleRelationships() throws Exception {
        try (Transaction tx = db.beginTx()) {
            MetaData person = MetaData.from(db, label("Person"));
            RelationshipInfo LIVES_ON_Location = new RelationshipInfo("LIVES_ON", "Location", true);
            assertEquals(map("LIVES_ON_Location", LIVES_ON_Location), person.relationships);
            MetaData location = MetaData.from(db, label("Location"));
            RelationshipInfo Person_LIVES_ON = new RelationshipInfo("LIVES_ON", "Person", false).update(true);
            RelationshipInfo User_LIVES_ON = new RelationshipInfo("LIVES_ON", "User", false).update(true);
            assertEquals(map("Person_LIVES_ON", Person_LIVES_ON,"User_LIVES_ON", User_LIVES_ON), location.relationships);
            tx.success();
        }
    }

    @Test
    public void allUsersQuery() throws Exception {
        Map<String, List<Map>> result = executeQuery("query UserQuery { User {id,name,age} User {age,name}}", map());
        assertEquals(2*5, result.get("User").size());
    }
    @Test
    public void allLocationsQuery() throws Exception {
        Map<String, List<Map>> result = executeQuery("query LocationQuery { Location {name} }", map());
        assertEquals(1, result.get("Location").size());
        assertEquals("Earth", result.get("Location").get(0).get("name"));
    }

    @Test
    public void singleUserWithLocationQuery() throws Exception {
        Map<String, List<Map>> result = executeQuery("query UserWithLocationQuery { User(id:3) {name,LIVES_ON_Location {name} } }", map());
        List<Map> users = result.get("User");
        assertEquals(1, users.size());
        Map user = users.get(0);
        assertEquals("John 3", user.get("name"));
        Map location = (Map) user.get("LIVES_ON_Location");
        assertEquals("Earth", location.get("name"));
    }
    @Test
    public void usersWithLocationQuery() throws Exception {
        Map<String, List<Map>> result = executeQuery("query UserWithLocationQuery { User {name,LIVES_ON_Location {name} } }", map());
        List<Map> users = result.get("User");
        assertEquals(5, users.size());
        Map user = users.get(0);
        assertEquals("John 1", user.get("name"));
        Map location = (Map) user.get("LIVES_ON_Location");
        assertEquals("Earth", location.get("name"));
    }
    @Test
    public void locationWithUsersQuery() throws Exception {
        Map<String, List<Map>> result = executeQuery("query LocationWithUserQuery { Location {name, User_LIVES_ON {name,age} } }", map());
        List<Map> locations = result.get("Location");
        assertEquals(1, locations.size());
        Map location = locations.get(0);
        assertEquals("Earth", location.get("name"));
        List<Map> people = (List<Map>) location.get("User_LIVES_ON");
        assertEquals(5, people.size());
        people.forEach((p) -> assertEquals(true, p.get("name").toString().startsWith("John")));
    }
    @Test
    public void locationWithPeopleQuery() throws Exception {
        Map<String, List<Map>> result = executeQuery("query LocationWithPersonQuery { Location {name, Person_LIVES_ON {name,age} } }", map());
        List<Map> locations = result.get("Location");
        assertEquals(1, locations.size());
        Map location = locations.get(0);
        assertEquals("Earth", location.get("name"));
        List<Map> people = (List<Map>) location.get("Person_LIVES_ON");
        assertEquals(5, people.size());
        people.forEach((p) -> assertEquals(true, p.get("name").toString().startsWith("John")));
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
