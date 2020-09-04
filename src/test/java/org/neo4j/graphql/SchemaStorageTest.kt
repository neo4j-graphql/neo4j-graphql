package org.neo4j.graphql

import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphql.TestUtil.execute
import org.neo4j.test.TestGraphDatabaseFactory

/**
 * @author mh
 * *
 * @since 05.05.17
 */
class SchemaStorageTest {

    @Before
    @Throws(Exception::class)
    fun setUp() {

        execute("CREATE (:Person {name:'Joe'})")
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        TestUtil.tearDown()
    }

    @Test
    fun resetOnCreateProperty() {
        val graphQL = SchemaStorage.schemaProperties(db!!)
        execute("CREATE (:Person {age:42})")
        Assert.assertNotSame(graphQL, SchemaStorage.getGraphQL(db!!))
    }

    @Test
    fun resetOnCreateLabel() {
        val graphQL = SchemaStorage.getGraphQL(db!!)
        db!!.execute("CREATE (:User {name:'Jane'})").close()
        Assert.assertNotSame(graphQL, SchemaStorage.getGraphQL(db!!))
    }

    @Test
    fun cacheBetweenInvocations() {
        val graphQL = SchemaStorage.getGraphQL(db!!)
        Assert.assertSame(graphQL, SchemaStorage.getGraphQL(db!!))
    }

    @Test
    fun noResetWithSameTokens() {
        val graphQL = SchemaStorage.getGraphQL(db!!)
        db!!.execute("CREATE (:Person {name:'Jane'})").close()
        Assert.assertSame(graphQL, SchemaStorage.getGraphQL(db!!))
    }

    @Test
    fun countSchemaElements() {

    }

    @Test
    fun reset() {
        val graphQL = SchemaStorage.getGraphQL(db!!)
        SchemaStorage.reset()
        Assert.assertNotSame(graphQL, SchemaStorage.getGraphQL(db!!))
    }

}
