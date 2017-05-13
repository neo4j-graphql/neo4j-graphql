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
        val arguments = listOf(
                Argument("names", ArrayValue(listOf(StringValue("Michael Hunger"), StringValue("Will Lyon")))),
                Argument("born", IntValue(BigInteger.valueOf(1960))))

        val field = Field("Person", arguments, selectionSet)

        val query = generator.generateQueryForField(field)

        assertEquals(
                """MATCH (`Person`:`Person`)
WHERE `Person`.`name` IN ["Michael Hunger","Will Lyon"]
AND `Person`.`born` = 1960
RETURN `Person`.`name` AS `name`,
`Person`.`born` AS `born`""",  query)
    }

    @Test
    @Throws(Exception::class)
    fun matchRelationship() {
        val metaData = IDLParser.parse("""
        type Person {
            name: String
            born: Int
            movies: [Movie] @relation(name: "ACTED_IN")
        }

        type Movie {
            title: String
        }
        """)

        GraphSchemaScanner.allTypes.putAll(metaData)

        val generator = Cypher31Generator()

        val selectionSet = SelectionSet(listOf<Selection>(Field("name"), Field("born"), Field("movies", SelectionSet(listOf(Field("title"))))))

        val field = Field("Person", selectionSet)

        val query = generator.generateQueryForField(field)

        assertEquals(
                """MATCH (`Person`:`Person`)
RETURN `Person`.`name` AS `name`,
`Person`.`born` AS `born`,
[ (`Person`)-[:`ACTED_IN`]->(`Person_movies`:`Movie`)  | `Person_movies` {.`title`}] AS `movies`""",  query)
    }

    @Test
    @Throws(Exception::class)
    fun cypherDirectiveScalar() {
        val metaData = IDLParser.parse("""
        type Person {
            name: String
            born: Int
            score: Int @cypher(statement: "WITH {this} AS this RETURN 2")
        }
        """)

        GraphSchemaScanner.allTypes.putAll(metaData)

        val generator = Cypher31Generator()

        val selectionSet = SelectionSet(listOf<Selection>(Field("score")))

        val field = Field("Person", selectionSet)

        val query = generator.generateQueryForField(field)

        assertEquals(
                """MATCH (`Person`:`Person`)
RETURN graphql.run('WITH {this} AS this RETURN 2', {`this`:Person}, false) AS `score`""",  query)
    }

    @Test
    @Throws(Exception::class)
    fun cypherDirectiveScalarArray() {
        val metaData = IDLParser.parse("""
        type Person {
            name: String
            born: Int
            scores: [Int] @cypher(statement: "UNWIND range(0,5) AS value RETURN value")
            scores2: [Int] @cypher(statement: "RETURN range(0,5)")
        }
        """)

        GraphSchemaScanner.allTypes.putAll(metaData)

        val generator = Cypher31Generator()

        val selectionSet = SelectionSet(listOf<Selection>(Field("scores"), Field("scores2")))

        val field = Field("Person", selectionSet)

        val query = generator.generateQueryForField(field)

        assertEquals(
                """MATCH (`Person`:`Person`)
RETURN graphql.run('UNWIND range(0,5) AS value RETURN value', {`this`:Person}, true) AS `scores`,
graphql.run('RETURN range(0,5)', {`this`:Person}, true) AS `scores2`""",  query)
    }

    @Test
    @Throws(Exception::class)
    fun cypherDirectiveTypedValue() {
        val metaData = IDLParser.parse("""
        type Person {
            name: String
            born: Int
            bestFriend: Person @cypher(statement: "WITH {this} AS this RETURN this")
        }
        """)

        GraphSchemaScanner.allTypes.putAll(metaData)

        val generator = Cypher31Generator()

        val selectionSet = SelectionSet(listOf<Selection>(Field("bestFriend", SelectionSet(listOf(Field("name"), Field("born"))))))

        val field = Field("Person", selectionSet)

        val query = generator.generateQueryForField(field)

        assertEquals(
                """MATCH (`Person`:`Person`)
RETURN head([ x IN graphql.run('WITH {this} AS this RETURN this', {`this`:Person}, true) | `x` {.`name`, .`born`} ]) AS `bestFriend`""",  query)
    }

    @Test
    @Throws(Exception::class)
    fun cypherDirectiveTypedArray() {
        val metaData = IDLParser.parse("""
        type Person {
            name: String
            born: Int
            colleagues: [Person] @cypher(statement: "WITH {this} AS this RETURN this")
        }
        """)

        GraphSchemaScanner.allTypes.putAll(metaData)

        val generator = Cypher31Generator()

        val selectionSet = SelectionSet(listOf<Selection>(Field("colleagues", SelectionSet(listOf(Field("name"), Field("born"))))))

        val field = Field("Person", selectionSet)

        val query = generator.generateQueryForField(field)
/*
query Person {
   name, born
}
 */

        assertEquals(
                """MATCH (`Person`:`Person`)
RETURN [ x IN graphql.run('WITH {this} AS this RETURN this', {`this`:Person}, true) | `x` {.`name`, .`born`} ] AS `colleagues`""",  query)
    }

    @Test
    @Throws(Exception::class)
    fun cypherParameterQuery() {
        val metaData = IDLParser.parse("""
        type Person {
            name: String
            born (value:Int!): Int @cypher(statement:"RETURN {value}")
        }
        """)

        GraphSchemaScanner.allTypes.putAll(metaData)

        val generator = Cypher31Generator()

        val selectionSet = SelectionSet(listOf(Field("name"), Field("born",listOf(Argument("value",IntValue(BigInteger.valueOf(7)))))))

        val field = Field("Person", selectionSet)

        val query = generator.generateQueryForField(field)

        assertEquals(
                """MATCH (`Person`:`Person`)
RETURN `Person`.`name` AS `name`,
graphql.run('RETURN {value}', {`this`:Person,`value`:7}, false) AS `born`""",  query)
    }

}
