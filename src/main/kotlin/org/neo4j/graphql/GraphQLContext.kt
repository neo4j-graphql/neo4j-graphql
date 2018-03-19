package org.neo4j.graphql

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.Log

class GraphQLContext(val db : GraphDatabaseService, val log : Log? = null, val parameters: Map<String,Any> = emptyMap(), val backLog : MutableMap<String,Any> = mutableMapOf()) {
    fun store(key : String, value : Any) {
        backLog[key]=value
    }
}
