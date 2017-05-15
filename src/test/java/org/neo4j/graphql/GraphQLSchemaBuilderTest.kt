package org.neo4j.graphql

import graphql.schema.GraphQLFieldDefinition
import org.junit.Test

import org.junit.Assert.*

class GraphQLSchemaBuilderTest {
    @Test
    fun mutationField() {
        val md = MetaData("Actor")
        md.addLabel("Person")
        md.addProperty("name",MetaData.PropertyType("String", nonNull = true))
        val mutationField: GraphQLFieldDefinition = GraphQLSchemaBuilder().mutationField(md)[0]
        println("mutationField = ${mutationField}")
        assertEquals("createActor", mutationField.name )
        println(mutationField.dataFetcher.toString())
        assertEquals(true, mutationField.dataFetcher.toString().contains("SET node:`Person`") )
    }

}
