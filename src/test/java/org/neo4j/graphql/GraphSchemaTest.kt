package org.neo4j.graphql

import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.TestGraphDatabaseFactory

/**
 * @author mh
 * *
 * @since 05.05.17
 */
class GraphSchemaTest {
    private var db: GraphDatabaseService? = null

    @Before
    @Throws(Exception::class)
    fun setUp() {
        db = TestGraphDatabaseFactory().newImpermanentDatabase()
        db!!.execute("CREATE (:Person {name:'Joe'})").close()
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        db!!.shutdown()
    }

    @Test
    fun resetOnCreateProperty() {
        val graphQL = GraphSchema.getGraphQL(db!!)
        db!!.execute("CREATE (:Person {age:42})").close()
        Assert.assertNotSame(graphQL, GraphSchema.getGraphQL(db!!))
    }

    @Test
    fun resetOnCreateLabel() {
        val graphQL = GraphSchema.getGraphQL(db!!)
        db!!.execute("CREATE (:User {name:'Jane'})").close()
        Assert.assertNotSame(graphQL, GraphSchema.getGraphQL(db!!))
    }

    @Test
    fun cacheBetweenInvocations() {
        val graphQL = GraphSchema.getGraphQL(db!!)
        Assert.assertSame(graphQL, GraphSchema.getGraphQL(db!!))
    }

    @Test
    fun noResetWithSameTokens() {
        val graphQL = GraphSchema.getGraphQL(db!!)
        db!!.execute("CREATE (:Person {name:'Jane'})").close()
        Assert.assertSame(graphQL, GraphSchema.getGraphQL(db!!))
    }

    @Test
    fun countSchemaElements() {

    }

    @Test
    fun reset() {
        val graphQL = GraphSchema.getGraphQL(db!!)
        GraphSchema.reset()
        Assert.assertNotSame(graphQL, GraphSchema.getGraphQL(db!!))
    }

}
