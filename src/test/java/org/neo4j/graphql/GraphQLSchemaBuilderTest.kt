package org.neo4j.graphql

import graphql.Scalars.GraphQLInt
import graphql.Scalars.GraphQLString
import graphql.language.ObjectTypeDefinition
import graphql.parser.Parser
import graphql.schema.*
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.InputMismatchException
import org.junit.Assert.assertEquals
import org.junit.Test
import java.util.*
import kotlin.test.assertNull
import kotlin.test.assertTrue

class GraphQLSchemaBuilderTest {
    @Test
    fun emptyNode() {
        GraphSchemaScanner.schema = null
        val md = MetaData("Actor")
        val schema = GraphQLSchemaBuilder(listOf(md))
        val type: GraphQLObjectType = schema.toGraphQLObjectType(md)
        assertEquals("Actor", type.name)
        assertEquals(listOf("_id"), type.fieldDefinitions.map { it.name })

        val queryType = schema.queryFields(listOf(md)).first()
        assertEquals("Actor", queryType.name)
        assertEquals(setOf("_id","_ids","first","offset"), queryType.arguments.map { it.name }.toSet())

        val graphQLSchema = schema.buildSchema()
        val ordering = graphQLSchema.getType("_ActorOrdering") as GraphQLEnumType?
        assertNull(ordering)
    }

    @Test
    fun mutationField() {
        GraphSchemaScanner.schema = null
        val md = MetaData("Actor")
        md.addLabel("Person")
        md.addProperty("name", MetaData.PropertyType("String", nonNull = 1))
        val mutationField: GraphQLFieldDefinition = GraphQLSchemaBuilder(listOf(md)).mutationField(md, emptySet())[0]
        println("mutationField = ${mutationField}")
        assertEquals("createActor", mutationField.name)
        println(mutationField.dataFetcher.toString())
//        assertEquals(true, mutationField.dataFetcher.toString().contains("SET node:`Person`"))
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
        assertEquals(listOf("Foo",42L,"Female"), type.fields.map { it.defaultValue })
        assertEquals(listOf(GraphQLString, GraphQLInt,enums.get("Gender")), type.fields.map { it.type })
    }

    @Test
    fun descriptions() {
        val idl = """
# GenderDesc
enum Gender {
 # FemaleDesc
 Female
}

# InputDesc
input Test {
 # InputNameDesc
 name: String = "Foo"
}

# TestObjectDesc
type TestObject {
   # nameDesc
   name: String
   # otherDesc
   other: [TestObject]
}
type QueryType {
   # someQueryDesc
   someQuery(
      # someQueryNameDesc
      name: String): [TestObject]
}
type MutationType {
   # someUpdateDesc
   someUpdate(
      # someUpdateNameDesc
      name: String): String
}

schema {
   query: QueryType
   mutation: MutationType
}
"""
        try {
            GraphSchemaScanner.schema = idl

            val document = Parser().parseDocument(idl)
            val builder = GraphQLSchemaBuilder(IDLParser.parse(idl).values)
            val schema = builder.buildSchema()

            val enum = builder.enumsFromDefinitions(document.definitions).values.first()
            assertEquals("GenderDesc", enum.description)
            assertEquals("FemaleDesc", enum.values.first().description)

            val inputType = builder.inputTypesFromDefinitions(document.definitions).values.first()
            assertEquals("InputDesc", inputType.description)
            assertEquals("InputNameDesc", inputType.fields.first().description)
            val mutation = schema.mutationType
            val someUpdate = mutation.getFieldDefinition("someUpdate")
            assertEquals("someUpdateDesc", someUpdate.description)
            assertEquals("someUpdateNameDesc", someUpdate.getArgument("name").description)

            val someQuery = schema.queryType.getFieldDefinition("someQuery")
            assertEquals("someQueryDesc", someQuery.description)
            assertEquals("someQueryNameDesc", someQuery.getArgument("name").description)

            val type = schema.getType("TestObject") as GraphQLObjectType
            assertEquals("TestObjectDesc", type.description)
            assertEquals("nameDesc", type.getFieldDefinition("name").description)
            assertEquals("otherDesc", type.getFieldDefinition("other").description)


        } catch (e:ParseCancellationException) {
            println("${e.message} cause: ${(e.cause as InputMismatchException?)?.offendingToken}")
            throw e
        }
    }

    @Test
    fun mandatoryRelationship() {
        val idl = """
type TestObject {
   name: String
   other: TestObject! @relation(name:"TEST")
}
"""
        try {
            GraphSchemaScanner.schema = idl

            val document = Parser().parseDocument(idl)
            val builder = GraphQLSchemaBuilder(IDLParser.parse(idl).values)
            val schema = builder.buildSchema()

            val other = (schema.getType("TestObject") as GraphQLObjectType).getFieldDefinition("other")
            assertTrue(other.type is GraphQLNonNull)
            assertEquals("TestObject",other.type.inner().name)
        } catch (e:ParseCancellationException) {
            println("${e.message} cause: ${(e.cause as InputMismatchException?)?.offendingToken}")
            throw e
        }
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
        assertEquals(listOf("Foo",42L, mapOf("female" to true)), type.fields.map { it.defaultValue })
        assertEquals(listOf("String", "Int", "Gender"), type.fields.map { it.type }.map { when (it) {
            is GraphQLScalarType -> it.name
            is GraphQLTypeReference -> it.name
            else -> null
        } })
        val queryFields = builder.toDynamicQueryOrMutationFields(definitions.filterIsInstance<ObjectTypeDefinition>().first().fieldDefinitions, inputTypes)
        assertEquals(type, queryFields.values.first().getArgument("test").type)
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
