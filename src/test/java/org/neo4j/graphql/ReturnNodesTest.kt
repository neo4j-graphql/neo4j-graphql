package org.neo4j.graphql

import graphql.GraphQL
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.TestGraphDatabaseFactory
import kotlin.test.assertEquals

/**
 * @author mh
 * *
 * @since 05.05.17
 */
class ReturnNodesTest {
    private lateinit var db: GraphDatabaseService
    private lateinit var ctx: GraphQLContext
    private var graphQL: GraphQL? = null


    @Before
    @Throws(Exception::class)
    fun setUp() {
        db = TestGraphDatabaseFactory().newImpermanentDatabase()
        (db as GraphDatabaseAPI).dependencyResolver.resolveDependency(Procedures::class.java).let {
            it.registerFunction(GraphQLProcedure::class.java)
            it.registerProcedure(GraphQLProcedure::class.java)
        }
        db.execute(data)?.close()

        ctx = GraphQLContext(db)
        GraphSchemaScanner.storeIdl(db, schema)
        graphQL = GraphSchema.getGraphQL(db)
    }

    val data = """
MERGE(start_node:Node { id: 1 })
MERGE(end_node:Node { id: 2 })

CREATE (start_node)-[e:Edge]->(end_node)
SET e.id = 1, e.distance=10.0

CREATE (start_node)-[:HasSubNode]->(:SubNode { id: 1, name: 'foo' })

CREATE (end_node)-[:HasSubNode]->(:SubNode { id: 2, name: 'bar' })
"""
    val schema = """
type Node {
    id: ID!
    subnodes: [SubNode] @relation(name:"HasSubNode")
    nodes: [Node] @relation(name:"Edge")
}

type SubNode {
    id: ID!
    name: String!
    nodes: [Node] @relation(name: "HasSubNode", direction: IN)
}

type Edge {
    id: ID!
    distance: Float!
}

schema {
    query: QueryType
}

type QueryType {
    shortestPath(fromSubNode: String!, toSubNode: String!): [Node] @cypher(statement:"MATCH (from:Node)-[:HasSubNode]-(:SubNode {name: ${"$"}fromSubNode}), (to:Node)-[:HasSubNode]-(:SubNode {name: ${"$"}toSubNode}) match p = shortestPath((from)-[*]->(to)) UNWIND nodes(p) as n RETURN distinct n")
}
"""

    @After
    @Throws(Exception::class)
    fun tearDown() {
        db.shutdown()
    }

    private fun assertResult(query: String, expected: Any, params: Map<String,Any> = emptyMap()) {
        val ctx2 = GraphQLContext(ctx.db, ctx.log, params)
        val result = graphQL!!.execute(query, ctx2, params)
        if (result.errors.isNotEmpty()) println(result.errors)
        assertEquals(expected, result.getData())
    }

    @Test
    fun findShortestPath() {
        val query = """
query {
  shortestPath(fromSubNode: "foo" toSubNode: "bar") {
    id
  }
}
"""
        assertResult(query, mapOf("shortestPath" to listOf(mapOf("id" to "1"),mapOf("id" to "2"))))
    }
}
