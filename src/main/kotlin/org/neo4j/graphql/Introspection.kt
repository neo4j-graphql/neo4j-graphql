package org.neo4j.graphql

import org.codehaus.jackson.JsonFactory
import org.codehaus.jackson.JsonGenerator
import org.codehaus.jackson.map.ObjectMapper
import java.net.HttpURLConnection
import java.net.URL

class Introspection {
    // todo mutations, etc.
    fun load(url: String, headers: Map<String, String>) : List<MetaData> {
        val result = postRequest(url, headers, mapOf<String,Any>("query" to INTROSPECTION, "variables" to mapOf<String,Any>()))
        return parseSchema(result)
    }

    private fun typeOf(typeInfo: Map<String,Any>, type:MetaData.PropertyType = MetaData.PropertyType("String")) : MetaData.PropertyType {
        return when (typeInfo["kind"]) {
            "NON_NULL" -> typeOf(typeInfo["ofType"] as Map<String,Any>, type.copy(nonNull = type.nonNull + 1))
            "LIST" -> typeOf(typeInfo["ofType"] as Map<String,Any>, type.copy(array = true))
            "OBJECT" -> type.copy(name=typeInfo["name"]!!.toString())
            "SCALAR" -> type.copy(name=typeInfo["name"]!!.toString())
            // todo enum, ...
            else -> type
        }
    }
    private fun parseSchema(schema: Map<String, Any>): List<MetaData> {
        val types = (schema["data"] as Map<String, Map<String, Any>>)["__schema"]!!["types"] as List<Map<String, Any>>
        return types.filter { it["kind"] == "OBJECT" && !it.getOrDefault("name","__").toString().startsWith("__") }.map {
            val m = MetaData(it["name"]!!.toString())
            m.description=it["description"]?.toString()
            it["fields"]?.let { fields ->  addFields(m, fields as List<Map<String, Any>>)}
            m
        }
    }

    private fun addFields(m: MetaData, fields: List<Map<String, Any>>) {
        fields.forEach {
            val type = typeOf(it["type"]!! as Map<String, Any>)
            val name = it["name"]!!.toString()
            val description = it["description"]?.toString()
            if (type.isBasic()) {
                m.addProperty(name, type = type, description = description)
                if (type.nonNull > 0) m.addIdProperty(name)
            } else {
                m.mergeRelationship(type.name, name, type.name, true, type.array, description, type.nonNull)
            }
        }
    }

    fun postRequest(url: String, headers:Map<String,String>, value: Map<String, Any>) : Map<String,Any> {
        val con = URL(url).openConnection() as HttpURLConnection
        con.requestMethod = "POST"
        con.addRequestProperty("content-type", "application/json")
        con.addRequestProperty("accept", "*/*")
        con.addRequestProperty("user-agent", "neo4j-graphql")
        headers.forEach(con::addRequestProperty)
        con.doInput = true
        con.doOutput = true
        MAPPER.writeValue(con.outputStream, value)
        @Suppress("UNCHECKED_CAST")
        return MAPPER.readValue(con.inputStream, Map::class.java) as Map<String,Any>
    }

    companion object {
        val MAPPER = ObjectMapper(JsonFactory().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, true))
        val INTROSPECTION =
        """
query IntrospectionQuery {
__schema {
  queryType { name }
  mutationType { name }
  subscriptionType { name }
  types {
    ...FullType
  }
  directives {
    name
    description
    args {
      ...InputValue
    }
  }
}
}

fragment FullType on __Type {
kind
name
description
fields(includeDeprecated: true) {
  name
  description
  args {
    ...InputValue
  }
  type {
    ...TypeRef
  }
  isDeprecated
  deprecationReason
}
inputFields {
  ...InputValue
}
interfaces {
  ...TypeRef
}
enumValues(includeDeprecated: true) {
  name
  description
  isDeprecated
  deprecationReason
}
possibleTypes {
  ...TypeRef
}
}

fragment InputValue on __InputValue {
name
description
type { ...TypeRef }
defaultValue
}

fragment TypeRef on __Type {
kind
name
ofType {
  kind
  name
  ofType {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
            }
          }
        }
      }
    }
  }
}
}

        """
    }
}
