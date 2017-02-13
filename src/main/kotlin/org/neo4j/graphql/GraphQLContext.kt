package org.neo4j.graphql

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.Log

class GraphQLContext(val db : GraphDatabaseService, val log : Log, val backLog : MutableMap<String,Any> = mutableMapOf()) {
    fun store(key : String, value : Any) {
        backLog[key]=value
    }
}
