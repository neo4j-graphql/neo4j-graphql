package org.neo4j.graphql

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.neo4j.graphql.TestUtil.assertResult
import org.neo4j.graphql.TestUtil.execute

/**
 * @author mh
 * *
 * @since 05.05.17
 */
class ReturnNodesTest {

    @Before
    @Throws(Exception::class)
    fun setUp() {
        execute(data)
        TestUtil.setup(schema)
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
    query: Query
}

type Query {
    shortestPath(fromSubNode: String!, toSubNode: String!): [Node] @cypher(statement:"MATCH (from:Node)-[:HasSubNode]-(:SubNode {name: ${"$"}fromSubNode}), (to:Node)-[:HasSubNode]-(:SubNode {name: ${"$"}toSubNode}) match p = shortestPath((from)-[*]->(to)) UNWIND nodes(p) as n RETURN distinct n")
}
"""

    @After
    @Throws(Exception::class)
    fun tearDown() {
        TestUtil.tearDown()
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
