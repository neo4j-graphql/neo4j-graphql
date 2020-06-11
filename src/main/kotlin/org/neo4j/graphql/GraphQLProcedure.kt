package org.neo4j.graphql

import apoc.result.VirtualNode
import apoc.result.VirtualRelationship
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.*
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import java.util.stream.Stream
import kotlin.streams.toList

/**
 * @author mh
 * @since 29.10.16
 */
class GraphQLProcedure {

    @Context
    @JvmField
    var db: GraphDatabaseService? = null

    @Context
    @JvmField
    var dbms: DatabaseManagementService? = null

    @Context
    @JvmField
    var log: Log? = null

    class GraphQLResult(@JvmField val result: Map<String, Any>)
    class GraphResult(@JvmField val nodes: List<Node>, @JvmField val rels: List<Relationship>)

    @Procedure("graphql.execute", mode = Mode.WRITE)
    fun execute(@Name("query") query: String, @Name("variables", defaultValue = "{}") variables: Map<String, Any>, @Name(value = "operation", defaultValue = "") operation: String?): Stream<GraphQLResult> {
        return doExecute(variables, query, operation)
    }

    @Procedure("graphql.query", mode = Mode.READ)
    fun query(@Name("query") query: String, @Name("variables", defaultValue = "{}") variables: Map<String, Any>, @Name(value = "operation", defaultValue = "") operation: String?): Stream<GraphQLResult> {
        return doExecute(variables, query, operation)
    }

    @Procedure("graphql.reset", mode = Mode.READ)
    fun reset() {
        return SchemaStorage.deleteSchema(dbms!!, db!!.databaseName())
    }

    private fun doExecute(variables: Map<String, Any>, query: String, operation: String?): Stream<GraphQLResult> {
        db!!.beginTx().use { tx ->
            try {
                val schemaIdl = SchemaStorage.schemaProperties(dbms!!, db!!.databaseName()).get("schema") as String?
                        ?: throw IllegalStateException("No Schema available for " + db!!.databaseName())
                val schemaConfig = SchemaConfig()
                val schema = SchemaBuilder.buildSchema(schemaIdl, schemaConfig)
                val queries = Translator(schema).translate(query, variables, QueryContext())
                // todo handle operation, return query-key/alias
                val results = queries.map { cypher -> tx.execute(cypher.query, cypher.params).let { r ->
                    println(cypher.query)
                    val isList = cypher.type?.isList() ?: false
                    val col = r.columns().first()
                    mapOf(col to r.columnAs<Any>(col).asSequence().let { if (isList) it.toList() else it.firstOrNull() })
                } }
                tx.commit()
                return Stream.of(GraphQLResult(mapOf("data" to results)))
            } catch (e: Exception) {
                tx.rollback()
                throw RuntimeException("Error executing GraphQL Query: $query", e)
            }
        }
    }

    data class StringResult(@JvmField val value: String?)

    @Procedure("graphql.idl", mode = Mode.WRITE)
    fun idl(@Name("idl") text: String?): Stream<StringResult> {
        if (text == null) {
            SchemaStorage.deleteSchema(dbms!!, db!!.databaseName())
            return Stream.of(StringResult("Removed stored GraphQL Schema"))
        } else {
            SchemaBuilder.buildSchema(text, SchemaConfig())
            SchemaStorage.updateSchema(dbms!!, db!!.databaseName(), text)
            return Stream.of(StringResult(text))
        }
    }

    @UserFunction("graphql.getIdl")
    fun getIdl() = SchemaStorage.schemaProperties(dbms!!, db!!.databaseName()).get("schema") as String?
            ?: throw RuntimeException("No GraphQL schema found for database " + db!!.databaseName())

    @Procedure("graphql.schema")
    fun schema(): Stream<GraphResult> {
        val idl = getIdl()
        val schema = SchemaBuilder.buildSchema(idl, SchemaConfig())
        val nodes = schema.allTypesAsList.associate { type ->
            val props = type.getInnerFieldsContainer().fieldDefinitions.filter { it.type.isScalar() || it.isNeo4jType() }.associate { it.name to it.type.isScalar() } + ("name" to type.name)
            type.name to VirtualNode(arrayOf(Label.label(type.name)), props)
        }
        val rels = schema.allTypesAsList.flatMap { type ->
            type.getInnerFieldsContainer().fieldDefinitions.filter { it.isRelationship() }.map { f ->
                VirtualRelationship(nodes[type.name], nodes[f.type.name], RelationshipType.withName(f.name))
                        .also { r ->
                            r.setProperty("type", f.getDirectiveArgument("relation", "name", null));
                            r.setProperty("multi", f.isList());
                        }
            }
        }

        return Stream.of(GraphResult(nodes.values.toList(), rels))
    }
}
