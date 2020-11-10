package org.neo4j.graphql

import apoc.result.VirtualNode
import apoc.result.VirtualRelationship
import graphql.introspection.Introspection
import graphql.schema.GraphQLObjectType
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.*
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import java.util.stream.Stream

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

    class GraphQLResult(@JvmField val result: Any)
    class GraphResult(@JvmField val nodes: List<Node>, @JvmField val rels: List<Relationship>)

    @Procedure("graphql.execute", mode = Mode.WRITE)
    fun execute(
            @Name("query") query: String,
            @Name("variables", defaultValue = "{}") variables: Map<String, Any>
    ): Stream<GraphQLResult> {
        return doExecute(variables, query)
    }

    @Procedure("graphql.query", mode = Mode.READ)
    fun query(
            @Name("query") query: String,
            @Name("variables", defaultValue = "{}") variables: Map<String, Any>
    ): Stream<GraphQLResult> {
        return doExecute(variables, query)
    }

    @Procedure("graphql.reset", mode = Mode.READ)
    fun reset() {
        SchemaStorage.deleteSchema(dbms!!, db!!.databaseName())
    }

    private fun doExecute(variables: Map<String, Any>, query: String): Stream<GraphQLResult> {
        val result = db!!.executeGraphQl(dbms!!, query, variables)
                ?: throw RuntimeException("No GraphQL schema found for database " + db!!.databaseName())
        if (result.errors.isEmpty()) {
            return Stream.of(GraphQLResult(result.getData()))
        }
        val errors = result.errors.joinToString("\n")
        throw RuntimeException("Error executing GraphQL Query:\n $errors")
    }

    data class StringResult(@JvmField val value: String?)

    @Suppress("unused")
    @Procedure("graphql.idl", mode = Mode.WRITE)
    fun idl(@Name("idl") text: String?): Stream<StringResult> {
        val result = if (text == null) {
            SchemaStorage.deleteSchema(dbms!!, db!!.databaseName())
            "Removed stored GraphQL Schema"
        } else {
            SchemaStorage.updateSchema(dbms!!, db!!.databaseName(), text)
            text
        }
        return Stream.of(StringResult(result))
    }

    @UserFunction("graphql.getIdl")
    fun getIdl() = SchemaStorage.getSchema(dbms!!, db!!.databaseName())
            ?: throw RuntimeException("No GraphQL schema found for database " + db!!.databaseName())

    @Suppress("unused")
    @UserFunction("graphql.getAugmentedSchema")
    fun getAugmentedSchema() = SchemaStorage.getAugmentedSchema(dbms!!, db!!.databaseName())
            ?.let { GraphQLSchemaResource.SCHEMA_PRINTER.print(it) }
            ?: throw RuntimeException("No GraphQL schema found for database " + db!!.databaseName())

    @Procedure("graphql.schema")
    fun schema(): Stream<GraphResult> {
        val schema = SchemaStorage.getAugmentedSchema(dbms!!, db!!.databaseName())
                ?: throw RuntimeException("No GraphQL schema found for database " + db!!.databaseName())
        val relevantTypes = schema.allTypesAsList
                .filterIsInstance<GraphQLObjectType>()
                .filterNot {
                    setOf(
                            schema.queryType,
                            schema.mutationType,
                            schema.subscriptionType,
                            Introspection.__Field,
                            Introspection.__Directive,
                            Introspection.__EnumValue,
                            Introspection.__InputValue,
                            Introspection.__Schema,
                            Introspection.__Type
                    ).contains(it) || it.isNeo4jType()
                }
        val nodes = relevantTypes.associate { type ->
            val props = type.fieldDefinitions
                    .filter { it.type.isScalar() || it.isNeo4jType() }
                    .associate { it.name to it.type.innerName() } + ("__name" to type.name)
            type.name to VirtualNode(arrayOf(Label.label(type.name)), props)
        }
        val rels = relevantTypes.flatMap { type ->
            type.fieldDefinitions.filter { it.isRelationship() }.map { f ->
                VirtualRelationship(nodes[type.name], nodes[f.type.innerName()], RelationshipType.withName(f.name))
                        .also { r ->
                            r.setProperty("type", f.getDirectiveArgument("relation", "name", null))
                            r.setProperty("multi", f.type.isList())
                        }

            }
        }

        return Stream.of(GraphResult(nodes.values.toList(), rels))
    }
}
