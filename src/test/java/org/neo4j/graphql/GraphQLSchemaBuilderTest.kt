package org.neo4j.graphql

import graphql.Scalars.GraphQLInt
import graphql.Scalars.GraphQLString
import graphql.language.ObjectTypeDefinition
import graphql.parser.Parser
import graphql.schema.*
import org.junit.Assert.assertEquals
import org.junit.Test

class GraphQLSchemaBuilderTest {
    @Test
    fun mutationField() {
        val md = MetaData("Actor")
        md.addLabel("Person")
        md.addProperty("name", MetaData.PropertyType("String", nonNull = true))
        val mutationField: GraphQLFieldDefinition = GraphQLSchemaBuilder(listOf(md)).mutationField(md)[0]
        println("mutationField = ${mutationField}")
        assertEquals("createActor", mutationField.name)
        println(mutationField.dataFetcher.toString())
        assertEquals(true, mutationField.dataFetcher.toString().contains("SET node:`Person`"))
    }

    @Test
    fun inputType() {
        val input = """
enum Gender { Female, Male }
input Test {
 name: String = "Foo"
 age: Int = 42
 sex: Gender = Female
}
"""
        val document = Parser().parseDocument(input)

        val builder = GraphQLSchemaBuilder(emptyList())
        val enums = builder.enumsFromDefinitions(document.definitions)
        val inputTypes = builder.inputTypesFromDefinitions(document.definitions,enums)
        assertEquals(1,inputTypes.size)
        val type = inputTypes["Test"]!!
        assertEquals("Test", type.name)
        assertEquals(listOf("name","age","sex"), type.fields.map { it.name })
        assertEquals(listOf("Foo",42,"Female"), type.fields.map { it.defaultValue })
        assertEquals(listOf(GraphQLString, GraphQLInt,enums.get("Gender")), type.fields.map { it.type })
    }

    @Test
    fun nestedInputType() {
        val input = """
input Gender { female: Boolean }
input Test {
 name: String = "Foo"
 age: Int = 42
 sex: Gender = { female: true }
}
type QueryType {
   field(test:Test) : String
}
"""
        val document = Parser().parseDocument(input)

        val builder = GraphQLSchemaBuilder(emptyList())
        val definitions = document.definitions
        val inputTypes = builder.inputTypesFromDefinitions(definitions)
        assertEquals(2,inputTypes.size)
        val type = inputTypes["Test"]!!
        assertEquals("Test", type.name)
        assertEquals(listOf("name","age","sex"), type.fields.map { it.name })
        assertEquals(listOf("Foo",42, mapOf("female" to true)), type.fields.map { it.defaultValue })
        assertEquals(listOf("String", "Int", "Gender"), type.fields.map { it.type }.map { when (it) {
            is GraphQLScalarType -> it.name
            is GraphQLTypeReference -> it.name
            else -> null
        } })
        val queryFields = builder.toDynamicQueryOrMutationFields(definitions.filterIsInstance<ObjectTypeDefinition>().first().fieldDefinitions, inputTypes)
        assertEquals(type, queryFields.first().getArgument("test").type)
    }
    @Test
    fun enums() {
        val input = """
enum Gender { Female, Male }
"""
        val document = Parser().parseDocument(input)

        val builder = GraphQLSchemaBuilder(emptyList())
        val enums = builder.enumsFromDefinitions(document.definitions)
        assertEquals(1,enums.size)
        val type = enums["Gender"]!!
        assertEquals("Gender", type.name)
        assertEquals(listOf("Female", "Male"), type.values.map { it.name })
    }
}
