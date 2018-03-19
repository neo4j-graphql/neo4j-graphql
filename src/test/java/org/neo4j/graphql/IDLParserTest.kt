package org.neo4j.graphql

import graphql.language.*
import graphql.parser.Parser
import org.junit.Test
import java.math.BigInteger
import kotlin.test.assertEquals
import kotlin.test.fail

/**
 * copied from graphql.parser.IDLParserTest
 */

class IDLParserTest {

    @Test
    fun parseIDLError() {
        val input = """
type Foo {
  name String
}
"""
        try {
            IDLParser.parse(input)
            fail("Should fail parsing")
        } catch(e: Exception) {
            assertEquals("Error parsing IDL expected ':' got 'String' line 3 column 7",e.message)
        }
    }

    @Test
    fun parseIDL() {
        val input = """
interface InterfaceName @interfaceDirective(argName1:${'$'}varName argName2:true) {
fieldName(arg1:SomeType={one:1} @argDirective(a1:${'$'}v1)):[Elm] @fieldDirective(cool:true)
}
"""
        val document = Parser().parseDocument(input)

        println(document.definitions[0])
        println(iface())
        assertEquals(1, document.definitions.size)
//        assert(document.definitions[0] == iface())
        assertEquals(iface().toString(), document.definitions[0].toString())
    }

    fun iface(): InterfaceTypeDefinition {
        val iface = InterfaceTypeDefinition("InterfaceName")
        iface.directives
                .add(Directive("interfaceDirective",
                        listOf(Argument("argName1", VariableReference("varName")),
                                Argument("argName2", BooleanValue(true)))))
        val field = FieldDefinition("fieldName", ListType(TypeName("Elm")))
        field.directives
                .add(Directive("fieldDirective", listOf(Argument("cool", BooleanValue(true)))))

        val defaultValue = ObjectValue()
        defaultValue.objectFields.add(ObjectField("one", IntValue(BigInteger.valueOf(1))))
        val arg1 = InputValueDefinition("arg1",
                TypeName("SomeType"),
                defaultValue)
        arg1.directives
                .add(Directive("argDirective", listOf(Argument("a1", VariableReference("v1")))))
        field.inputValueDefinitions.add(arg1)

        iface.fieldDefinitions.add(field)
        return iface
    }

    @Test
    fun simpleSchemaTest() {
        val schema = "type Human { name: String }"

        val metaDatas = IDLParser.parse(schema)
        assertEquals(setOf("Human"), metaDatas.keys)
        val md = metaDatas["Human"]!!
        assertEquals(setOf("name"), md.properties.keys)
    }

    val simple = """type Person { name: String }"""

    @Test
    fun veryVerySimple() {
        val metaDatas = IDLParser.parse(simple)
        // todo handle enums ?
        println(metaDatas)

    }

    val starWars = """
enum Episode { NEWHOPE, EMPIRE, JEDI }

interface Character {
    id: String!,
    name: String,
    friends: [Character],
    appearsIn: [Episode],
}

type Human implements Character {
    id: String!,
    name: String,
    friends: [Character],
    appearsIn: [Episode],
    homePlanet: String,
}

type Droid implements Character {
    id: ID!,
    name: String,
    friends: [Character],
    appearsIn: [Episode],
    primaryFunction: String,
    test1: Boolean,
    test2: Number,
    test3: [Number],
    test4: Int,
}
"""

    @Test
    fun idlToMeta() {
        val metaDatas = IDLParser.parse(starWars)
        // todo handle enums ?
        println(metaDatas)
        assertEquals(metaDatas.keys, setOf("Character", "Droid", "Human"))
        val md = metaDatas["Human"]!!
        assertEquals(setOf("Character"), md.labels)
        assertEquals(setOf("id", "name", "appearsIn", "homePlanet"), md.properties.keys)
    }

    //  # (direction: Direction = OUT, type: String =  "ACTED_IN"): [Movie]
    //   # (direction: Direction = IN, type: String = "ACTED_IN"): [Person]
    //

    val moviesSchema = """
type Person {
   name: String!
   born: Int
   movies: [Movie] @relation(name:"ACTED_IN")
}

type Movie {
    title: String!
    released: Int
    tagline: String
    actors: [Person] @relation(name:"ACTED_IN", direction:"IN")
    score(value:Int! = 1): Int @cypher(statement:"RETURN {value}")
}
"""


    /*
    Person(name = "Tom Hanks") {
      born
      movies {
        title
        released
        tagline
        actors {
            name
        }
      }
    }
     */

    @Test
    fun moviesSchema() {
        val metaDatas = IDLParser.parse(moviesSchema)
        // todo handle enums ?
        println(metaDatas)
        println(metaDatas.keys)
        assertEquals(setOf("Movie", "Person"), metaDatas.keys)
        val md = metaDatas["Movie"]!!
        println(md.properties.keys)
        assertEquals(md.properties.keys, setOf("title", "released", "tagline", "score"))
        val scoreInfo = md.properties["score"]!!
        assertEquals(scoreInfo.parameters, mapOf("value" to MetaData.ParameterInfo("value", MetaData.PropertyType("Int", nonNull = 1), 1L)))
        assertEquals(scoreInfo.cypher?.cypher, "RETURN {value}")
        assertEquals(md.relationships.keys, setOf("actors"))
        println(md.relationships.values)
        assertEquals(md.relationships.values.iterator().next(), MetaData.RelationshipInfo("actors", "ACTED_IN", "Person", false, true))
    }


    @Test
    fun parseEnum() {
        val input = """
enum Test {
 A, B, C
}
"""
        val document = Parser().parseDocument(input)

        println(document.definitions)
        assertEquals(1, document.definitions.size)
        val enums = IDLParser.parseEnums(Parser().parseDocument(input).definitions)
        assertEquals(1, enums.size)
        assertEquals(listOf("A","B","C"), enums["Test"])
    }

    @Test
    fun parseInputType() {
        val input = """
input Test {
 name: String = "Foo"
 age: Int = 42
}
"""
        val document = Parser().parseDocument(input)

        println(document.definitions)
        assertEquals(1, document.definitions.size)
        val inputTypes = IDLParser.parseInputTypes(document.definitions)
        assertEquals(1, inputTypes.size)
        assertEquals(listOf("name","age"), inputTypes["Test"]?.map { it.name })
    }

    @Test
    fun parseMutations() {
        val input = """
schema {
   mutation: MutationType
}
type MutationType {
    test(name: String) : String @cypher(statement:"CREATE (t:Test {name:{name}}) RETURN id(t)")
    newPerson(name:ID!, born:Int) : String @cypher(statement:"CREATE (:Person {name:{name},born:{born}})")
    newMovie(title:ID!, released:Int, tagline:String) : Movie @cypher(statement:"MERGE (m:Movie {title:{title}}) ON CREATE SET m += {released:{released}, tagline:{tagline}} RETURN m")
}
"""
        val document = Parser().parseDocument(input)

        println(document.definitions)
        assertEquals(2, document.definitions.size)
        val mutations = IDLParser.parseMutations(input)
        assertEquals(3, mutations.size)
    }
    @Test
    fun parseQueries() {
        val input = """
schema {
   query: QueryType
}
type QueryType {
    personByName(name:ID!) : Person @cypher(statement:"MATCH (p:Person {name:{name}}) RETURN p")
}
"""
        val document = Parser().parseDocument(input)

        println(document.definitions)
        assertEquals(2, document.definitions.size)
        val queries = IDLParser.parseQueries(input)
        assertEquals(1, queries.size)
    }

    val fancyMoviesSchema = """
interface Person {
   name: String!
   born: Int
   movies: [Movie]
}

type Actor implements Person {
   name: String!
   born: Int
   movies: [Movie] @relation(name:"ACTED_IN")
}

type Director implements Person {
   name: String!
   born: Int
   movies: [Movie] @relation(name:"DIRECTED")
}

type Movie {
    title: String! @isUnique
    released: Int @defaultValue(value: 1900)
    tagline: String
    actors: [Actor] @relation(name:"ACTED_IN", direction:"IN")
    directors: [Director] @relation(name:"DIRECTED", direction:"IN")
    score(value:Int! = 1): Int @cypher(statement:"RETURN {value}")
}
"""

    @Test
    fun fancyMoviesSchema() {
        val metaDatas = IDLParser.parse(fancyMoviesSchema)
        // todo handle enums ?
        println(metaDatas)
        println(metaDatas.keys)
        assertEquals(1900L, metaDatas.get("Movie")?.properties?.get("released")?.defaultValue)
        assertEquals(true, metaDatas.get("Movie")?.properties?.get("title")?.unique)
    }

}
