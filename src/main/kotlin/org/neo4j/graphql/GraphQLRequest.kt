package org.neo4j.graphql

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSetter
import com.fasterxml.jackson.databind.ObjectMapper

@JsonIgnoreProperties(ignoreUnknown = true)
class GraphQLRequest @JsonCreator constructor(@JsonProperty("query") val query: String) {

    var operationName: String? = null

    var variables: Map<String, Any?> = mutableMapOf()

    constructor(query: String, variables: Map<String, Any>) : this(query) {
        this.variables = variables
    }

    constructor(query: String, variables: String?) : this(query) {
        setVariables(variables)
    }

    @JsonSetter
    fun setVariables(value: Any?) {
        @Suppress("UNCHECKED_CAST")
        variables = when (value) {
            is String -> parseMap(value)
            is Map<*, *> -> value as Map<String, Any>
            else -> emptyMap()
        }
    }

    companion object {
        private val OBJECT_MAPPER: ObjectMapper = ObjectMapper()

        @Suppress("UNCHECKED_CAST")
        private fun parseMap(value: String?): Map<String, Any> =
            if (value == null || value.isNullOrBlank() || value == "null") emptyMap()
            else {
                val v = value.trim('"', ' ', '\t', '\n', '\r')
                OBJECT_MAPPER.readValue(v, Map::class.java) as Map<String, Any>
            }
    }

}
