package org.neo4j.graphql

import graphql.language.*
import org.junit.Assert.*
import org.junit.Test
import java.math.BigInteger

class Cypher31GeneratorTest {

    @Test
    @Throws(Exception::class)
    fun basicMatch() {

        val metaData = IDLParser.parse("""
        type Person {
            name: String
        }
        """)

        GraphSchemaScanner.allTypes.putAll(metaData)

        val generator = Cypher31Generator()

        val field = Field("Person", SelectionSet(listOf<Selection>(Field("name"))))

        val query = generator.generateQueryForField(field)

        assertEquals(
"""MATCH (`Person`:`Person`)
RETURN `Person`.`name` AS `name`""",  query)
    }

    @Test
    @Throws(Exception::class)
    fun matchWhere() {

        val metaData = IDLParser.parse("""
        type Person {
            name: String
        }
        """)

        GraphSchemaScanner.allTypes.putAll(metaData)

        val generator = Cypher31Generator()

        val name = Field("name" )
        val field = Field("Person", listOf(Argument("name", StringValue("Michael Hunger"))), SelectionSet(listOf<Selection>(name)))

        val query = generator.generateQueryForField(field)

        assertEquals(
                """MATCH (`Person`:`Person`)
WHERE `Person`.`name` = "Michael Hunger"
RETURN `Person`.`name` AS `name`""",  query)
    }

    @Test
    @Throws(Exception::class)
    fun matchWhereMultipleArguments() {

        val metaData = IDLParser.parse("""
        type Person {
            name: String
            born: Int
        }
        """)

        GraphSchemaScanner.allTypes.putAll(metaData)

        val generator = Cypher31Generator()

        val selectionSet = SelectionSet(listOf<Selection>(Field("name"), Field("born")))
        val arguments = listOf(Argument("name", StringValue("Michael Hunger")), Argument("born", IntValue(BigInteger.valueOf(1960))))

        val field = Field("Person", arguments, selectionSet)

        val query = generator.generateQueryForField(field)

        assertEquals(
                """MATCH (`Person`:`Person`)
WHERE `Person`.`name` = "Michael Hunger"
AND `Person`.`born` = 1960
RETURN `Person`.`name` AS `name`,
`Person`.`born` AS `born`""",  query)
    }

}
