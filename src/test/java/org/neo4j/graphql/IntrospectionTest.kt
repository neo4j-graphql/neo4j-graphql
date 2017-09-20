package org.neo4j.graphql

import org.junit.Test

import org.junit.Assert.*

/**
 * @author mh
 * *
 * @since 26.04.17
 */
class IntrospectionTest {
    // works:
    // curl -i -X POST  -H accept:"*/*" -H content-type:application/json -d '{"query":"{ \ntwitter { \nuser (identifier: name, identity: \"clayallsopp\") { \nname \n} \n} \n}","variables":null}' https://www.graphqlhub.com/graphql
    @Test
    fun load() {
        val query = "{ \ntwitter { \nuser (identifier: name, identity: \"clayallsopp\") { \nname \n} \n} \n}"
        val metaData = Introspection().postRequest("https://www.graphqlhub.com/graphql", emptyMap(),mapOf("query" to query, "variables" to emptyMap<String,Any>()))
        println(metaData)
        val metaDatas = Introspection().load("https://www.graphqlhub.com/graphql", emptyMap())
        println(metaDatas)
    }
}
