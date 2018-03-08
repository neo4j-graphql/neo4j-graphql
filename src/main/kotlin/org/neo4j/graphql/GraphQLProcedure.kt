package org.neo4j.graphql

import graphql.ExecutionInput
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphql.procedure.VirtualNode
import org.neo4j.graphql.procedure.VirtualRelationship
import org.neo4j.logging.Log
import org.neo4j.procedure.*
import java.util.*
import java.util.stream.Stream

/**
 * @author mh
 * @since 29.10.16
 */
class GraphQLProcedure {

    @Context
    @JvmField var db: GraphDatabaseService? = null

    @Context
    @JvmField var log: Log? = null

    class GraphQLResult(@JvmField val result: Map<String, Any>)
    class GraphResult(@JvmField val nodes: List<Node>,@JvmField val rels: List<Relationship>)

    @Procedure("graphql.execute", mode = Mode.WRITE)
    fun execute(@Name("query") query : String , @Name("variables",defaultValue = "{}") variables : Map<String,Any>, @Name(value = "operation",defaultValue = "") operation: String?) : Stream<GraphQLResult> {
        return doExecute(variables, query, operation)
    }

    @Procedure("graphql.query", mode = Mode.READ)
    fun query(@Name("query") query : String , @Name("variables",defaultValue = "{}") variables : Map<String,Any>, @Name(value = "operation",defaultValue = "") operation: String?) : Stream<GraphQLResult> {
        return doExecute(variables, query, operation)
    }

    private fun doExecute(variables: Map<String, Any>, query: String, operation: String?): Stream<GraphQLResult> {
        val ctx = GraphQLContext(db!!, log!!, variables)
        val execution = ExecutionInput.Builder()
                .query(query).variables(variables).context(ctx).root(ctx) // todo proper mutation root
        if (!operation.isNullOrBlank()) execution.operationName(operation)

        val result = GraphSchema.getGraphQL(db!!).execute(execution.build())

        if (result.errors.isEmpty()) {
            return Stream.of(GraphQLResult(result.getData()))
        }
        if (ctx.backLog.isNotEmpty()) {
            // todo report metadata
        }
        val errors = result.errors.joinToString("\n")
        throw RuntimeException("Error executing GraphQL Query:\n $errors")
    }

    data class StringResult(@JvmField val value: String?)

    @Procedure("graphql.idl", mode = Mode.WRITE)
    fun idl(@Name("idl") idl: String?) : Stream<StringResult> {
        if (idl==null) {
            GraphSchemaScanner.deleteIdl(db!!)
            return Stream.of(StringResult("Removed stored GraphQL Schema"))
        } else {
            val storeIdl = GraphSchemaScanner.storeIdl(db!!, idl)
            return Stream.of(StringResult(storeIdl.toString()))
        }
    }

    @Procedure("graphql.schema")
    fun schema() : Stream<GraphResult> {
        GraphSchemaScanner.databaseSchema(db!!)
        val metaDatas = GraphSchemaScanner.allMetaDatas()

        val nodes = metaDatas.associate {
            val props = it.properties.values.associate { " "+it.fieldName to it.type.toString() } + ("name" to it.type)
            it.type to VirtualNode(listOf(it.type) + it.labels, props)
        }
        val rels = metaDatas.flatMap { n ->
            val node = nodes[n.type]!!
            n.relationships.values.map { rel ->
                VirtualRelationship(node, rel.fieldName, mapOf("type" to rel.type, "multi" to rel.multi), nodes[rel.label]!!)
            }
        }
        return Stream.of(GraphResult(nodes.values.toList(),rels))
    }

    @UserFunction("graphql.run")
    fun run(@Name("query") query: String, @Name("variables",defaultValue = "{}") variables : Map<String,Any>, @Name("expectMultipleValues",defaultValue = "true") expectMultipleValues : Boolean) : Any {
        val result = db!!.execute(query, variables)

        val firstColumn = result.columns()[0]

        if(expectMultipleValues) {
            return result.columnAs<Any>(firstColumn).asSequence().toList()
        } else {
            return result.columnAs<Any>(firstColumn).next()!!
        }
    }

    @UserFunction("graphql.labels")
    fun labels(@Name("entity") entity: Any) : List<String> {
        return when (entity) {
            is Node -> entity.labels.map { it.name() }
            is Relationship -> listOf(entity.type.name())
            is Map<*,*> -> entity.get("_labels") as List<String>? ?: emptyList<String>()
            else -> emptyList()
        }
    }

    data class Row(@JvmField val row:Any?)

    @Procedure("graphql.run", mode = Mode.WRITE)
    fun runProc(@Name("query") query: String, @Name("variables",defaultValue = "{}") variables : Map<String,Any>, @Name("expectMultipleValues", defaultValue = "true") expectMultipleValues : Boolean) : Stream<Row> {
        val result = run(query, variables, expectMultipleValues)

        return if (result is List<*>) {
            result.stream().map { Row(it) }
        } else {
            Stream.of(Row(result))
        }
    }

    data class Nodes(@JvmField val node:Node?)
    @Procedure("graphql.queryForNodes")
    fun queryForNodes(@Name("query") query: String, @Name("variables",defaultValue = "{}") variables : Map<String,Any>) : Stream<Nodes> {
        val result = db!!.execute(query, variables)
        val firstColumn = result.columns()[0]
        return result.columnAs<Node>(firstColumn).stream().map{ Nodes(it) }
    }

    @Procedure("graphql.updateForNodes",mode = Mode.WRITE)
    fun updateForNodes(@Name("query") query: String, @Name("variables",defaultValue = "{}") variables : Map<String,Any>) : Stream<Nodes> {
        val result = db!!.execute(query, variables)
        val firstColumn = result.columns()[0]
        return result.columnAs<Node>(firstColumn).stream().map{ Nodes(it) }
    }

    @UserFunction("graphql.sortColl")
    fun sortColl(@Name("coll") coll : java.util.List<Map<String,Any>>,
                 @Name("orderFields", defaultValue = "[]") orderFields : java.util.List<String>,
                 @Name("limit", defaultValue = "-1") limit : Long,
                 @Name("skip", defaultValue = "0") skip : Long): List<Map<String,Any>> {

        val fields = orderFields.map { it: String -> val asc = it[0] == '^'; Pair(if (asc) it.substring(1) else it, asc) }

        val result = ArrayList(coll) as List<Map<String,Comparable<Any>>>

        val compare = { o1: Map<String, Comparable<Any>>, o2: Map<String, Comparable<Any>> ->
            // short-cut the folding
            fields.fold(0) { a: Int, s: Pair<String, Boolean> ->
                if (a == 0) {
                    val name = s.first
                    val v1 = o1.get(name)
                    val v2 = o2.get(name)
                    if (v1 == v2) 0
                    else {
                        val cmp = if (v1 == null) -1 else if (v2 == null) 1 else v1.compareTo(v2)
                        if (s.second) cmp else -cmp
                    }
                } else a
            }
        }

        Collections.sort(result, compare)

        return (if (skip > 0 && limit != -1L) result.subList (skip.toInt(), skip.toInt() + limit.toInt())
        else if (skip > 0) result.subList (skip.toInt(), result.size)
        else if (limit != -1L) result.subList (0, limit.toInt())
        else result)
    }

    @Procedure("graphql.introspect")
    fun introspect(@Name("url") url:String, @Name("headers",defaultValue = "{}") headers:Map<String,String>) : Stream<GraphResult> {
        val metaDatas = Introspection().load(url, headers)
        // todo store as idl ?
        val nodes = metaDatas.associate {
            val props = it.properties.values.associate { " "+it.fieldName to it.type.toString() } + ("name" to it.type)
            it.type to VirtualNode(listOf(it.type) + it.labels, props)
        }
        val rels = metaDatas.flatMap { n ->
            val node = nodes[n.type]!!
            n.relationships.values.map { rel ->
                nodes[rel.label]?.let { labelNode ->
                    val (start, end) = if (rel.out) node to labelNode!! else labelNode!! to node
                    VirtualRelationship(start, rel.fieldName, mapOf("type" to rel.type, "multi" to rel.multi), end)
                }
            }.filterNotNull()
        }
        return Stream.of(GraphResult(nodes.values.toList(),rels))
    }

}
