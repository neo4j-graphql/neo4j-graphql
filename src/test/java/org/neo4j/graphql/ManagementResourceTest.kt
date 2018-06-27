package org.neo4j.graphql

import graphql.Scalars
import graphql.schema.GraphQLList
import graphql.schema.GraphQLNonNull
import graphql.schema.GraphQLObjectType
import graphql.schema.GraphQLSchema
import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.FormattedLogProvider
import org.neo4j.test.TestGraphDatabaseFactory
import kotlin.test.assertEquals

/**
 * @author mh
 * *
 * @since 15.06.18
 */
class ManagementResourceTest {

    private val log = FormattedLogProvider.toOutputStream(System.out)
    lateinit var db: GraphDatabaseService
    lateinit var procedureSchema: GraphQLSchema

    @Before
    fun setUp() {
        db = TestGraphDatabaseFactory().newImpermanentDatabase()
        procedureSchema = ManagementResource(log,db).procedureSchema()
    }

    @After
    fun tearDown() {
        db.shutdown()
    }


    @Test
    fun readProceduresSchema() {
        procedureSchema.queryType.fieldDefinitions.forEach { println(it.name) }
        assertTrue(procedureSchema.queryType.fieldDefinitions.any { it.name == "dbLabels" })
        val label = procedureSchema.queryType.fieldDefinitions.first { it.name == "dbLabels" }
        assertTrue(label.arguments.isEmpty())
        println(label.type.name)
        assertTrue(label.type.isList())
        val resultType = (label.type as GraphQLList).wrappedType
        assertTrue(resultType is GraphQLObjectType)
        val fields = (resultType as GraphQLObjectType).fieldDefinitions
        assertEquals(1, fields.size)
        assertEquals("label", fields.single().name)
        assertEquals(Scalars.GraphQLString, fields.single().type)
        val names = procedureSchema.queryType.fieldDefinitions.map { it.name }.toSet()
        assertTrue("expected procedures", listOf("dbAwaitIndex","dbConstraints","dbIndexExplicitSeekNodes","dbmsComponents").all { names.contains(it) })
    }

    @Test
    fun writeProceduresSchema() {
        val mutationType = procedureSchema.mutationType
        mutationType.fieldDefinitions.forEach { println(it.name) }
        assertTrue(mutationType.fieldDefinitions.any { it.name == "dbCreateLabel" })
        val label = mutationType.fieldDefinitions.first { it.name == "dbCreateLabel" }
        assertEquals(1, label.arguments.size)
        assertTrue(label.type == Scalars.GraphQLBoolean) // void
        val names = mutationType.fieldDefinitions.map { it.name }.toSet()
        assertTrue("expected procedures", listOf("dbIndexExplicitDrop","dbIndexExplicitRemoveRelationship","dbCreateProperty").all { names.contains(it) })
    }
    @Test
    fun writeProceduresIndexAddRelationship() {
        val mutationType = procedureSchema.mutationType
        val index = mutationType.fieldDefinitions.first { it.name == "dbIndexExplicitAddRelationship" }
        assertEquals(4, index.arguments.size)
        val names = listOf("indexName", "key", "value").toSet()
        assertEquals(names + "relationship",index.arguments.map { it.name }.toSet())
        assertTrue(index.arguments.filter { it.name in names }.all { it.type == GraphQLNonNull(Scalars.GraphQLString) })
        val arg = index.arguments.first { it.name == "relationship" }
        assertEquals("AttributeInput", (((arg.type as GraphQLNonNull).wrappedType as GraphQLList).wrappedType).name)
        assertTrue(index.type.isList())
        val resultType = (index.type as GraphQLList).wrappedType
        assertTrue(resultType is GraphQLObjectType)
        val fields = (resultType as GraphQLObjectType).fieldDefinitions
        assertEquals(1, fields.size)
        assertEquals("success", fields.single().name)
        assertTrue(fields.single().type == Scalars.GraphQLBoolean)
    }

    @Test
    fun executeLabel() {
        db.execute("CREATE (:Foo)").close()
        val result = ManagementResource(log, db).executeQuery(mapOf("query" to "{dbLabels { label } }"))
        assertEquals(mapOf("dbLabels" to listOf(mapOf("label" to "Foo"))),result["data"])
    }

    @Test
    fun executeComponents() {
        val result = ManagementResource(log, db).executeQuery(mapOf("query" to "{dbmsComponents { name, edition } }"))
        assertEquals(mapOf("dbmsComponents" to listOf(mapOf("name" to "Neo4j Kernel", "edition" to "community"))),result["data"])
    }
    @Test
    fun executeJmx() {
        val result = ManagementResource(log, db).executeQuery(mapOf("query" to """{dbmsQueryJmx(query:"org.neo4j:name=Kernel,*") { name, description, attributes {key, value} } }"""))
        println(result)
        val data = ((result["data"] as Map<*,*>)["dbmsQueryJmx"] as List<*>).single() as Map<*,*>
        assertEquals("org.neo4j:instance=kernel#0,name=Kernel", data["name"])
        assertTrue((data["description"] as String).contains("about the Neo4j kernel"))
        assertTrue((data["attributes"].toString()).contains("{key=KernelVersion, value={description=The version of Neo4j, value=neo4j-kernel, version: 3."))
    }


    @Test
    fun executeCreateLabel() {
        val result = ManagementResource(log, db).executeQuery(mapOf("query" to """mutation {res:dbCreateLabel(newLabel:"Bar")}"""))
        assertEquals(mapOf("res" to true),result["data"])
    }
    @Test
    fun executeCreateIndex() {
        val result = ManagementResource(log, db).executeQuery(mapOf("query" to """mutation {res:dbIndexExplicitForNodes(indexName:"Foo") {type,name,config { key, value, type }}}"""))
        val config = listOf(mapOf("key" to "type", "value" to "exact", "type" to "String"), mapOf("key" to "provider", "value" to "lucene", "type" to "String"))
        assertEquals(mapOf("res" to listOf(mapOf("type" to "NODE", "name" to "Foo", "config" to config))),result["data"])
    }

    @Test
    fun executeCreateIndexWithConfig() {
        val result = ManagementResource(log, db).executeQuery(mapOf("query" to """mutation {res:dbIndexExplicitForNodes(indexName:"Bar",config:[{key:"type",value:"fulltext"},{key:"provider",value:"lucene"}]) {type,name,config { key, value, type }}}""")) // ,{key:"foo",value:"42",type:Integer}
        val config = listOf(mapOf("key" to "type", "value" to "fulltext", "type" to "String"), mapOf("key" to "to_lower_case", "value" to "true", "type" to "String"), mapOf("key" to "provider", "value" to "lucene", "type" to "String"))
        assertEquals(mapOf("res" to listOf(mapOf("type" to "NODE", "name" to "Bar", "config" to config))),result["data"])
    }
}
