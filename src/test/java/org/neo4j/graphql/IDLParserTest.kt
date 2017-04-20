package org.neo4j.graphql

import graphql.language.*
import graphql.parser.Parser
import org.junit.Test
import java.math.BigInteger

/**
 * copied from graphql.parser.IDLParserTest
 */

class IDLParserTest {

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
        assert(document.definitions.size == 1)
//        assert(document.definitions[0] == iface())
        assert(document.definitions[0].toString() == iface().toString())
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
        assert(setOf("Human") == metaDatas.keys)
        val md = metaDatas["Human"]!!
        assert(md.properties.keys == setOf("name"))
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
        assert(setOf("Character", "Droid", "Human") == metaDatas.keys)
        val md = metaDatas["Human"]!!
        assert(md.labels == setOf("Character"))
        assert(md.properties.keys == setOf("id", "name", "homePlanet"))
    }

    //  # (direction: Direction = OUT, type: String =  "ACTED_IN"): [Movie]
    //   # (direction: Direction = IN, type: String = "ACTED_IN"): [Person]
    //

    val moviesSchema = """
enum Direction {
   OUT, IN, BOTH
}

type Person {
   name: String!
   born: Int
   movies: [Movie] @out(name:"ACTED_IN")
}

type Movie {
    title: String!
    released: Int
    tagline: String
    actors: [Person] @in(name:"ACTED_IN")
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
        assert(setOf("Movie","Person") == metaDatas.keys)
        val md = metaDatas["Movie"]!!
        println(md.properties.keys)
        assert(md.properties.keys == setOf("title","released","tagline"))
        assert(md.relationships.keys == setOf("actors"))
        println(md.relationships.values)
        assert(md.relationships.values.iterator().next() == RelationshipInfo("actors","ACTED_IN","Person",false))
    }

}
