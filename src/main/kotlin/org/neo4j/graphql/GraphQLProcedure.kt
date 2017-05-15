package org.neo4j.graphql

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

    @Procedure("graphql.execute")
    fun execute(@Name("query") query : String , @Name("variables") variables : Map<String,Any>) : Stream<GraphQLResult> {
        val ctx = GraphQLContext(db!!, log!!)
        val result = GraphSchema.getGraphQL(db!!).execute(query, ctx, variables)

        if (result.errors.isEmpty()) {
            return Stream.of(GraphQLResult(result.getData()))
        }
        if (ctx.backLog.isNotEmpty()) {
            // todo report metadata
        }
        val errors = result.errors.joinToString("\n")
        throw RuntimeException("Error executing GraphQL Query:\n $errors")
    }


    data class StringResult(@JvmField val value: String)

    @Procedure("graphql.idl", mode = Mode.WRITE)
    fun idl(@Name("idl") idl: String) : Stream<StringResult> {
        val storeIdl = GraphSchemaScanner.storeIdl(db!!, idl)
        return Stream.of(StringResult(storeIdl.toString()))
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
                val (start, end) = if (rel.out) node to nodes[rel.label]!! else nodes[rel.label]!! to node
                VirtualRelationship(start, rel.fieldName, mapOf("type" to rel.type, "multi" to rel.multi), end)
            }
        }
        return Stream.of(GraphResult(nodes.values.toList(),rels))
    }

    @UserFunction("graphql.run")
    fun run(@Name("query") query: String, @Name("variables") variables : Map<String,Any>, @Name("expectMultipleValues") expectMultipleValues : Boolean) : Any {
        val result = db!!.execute(query, variables)

        val firstColumn = result.columns()[0]

        if(expectMultipleValues) {
            return result.columnAs<Any>(firstColumn).asSequence().toList()
        } else {
            return result.columnAs<Any>(firstColumn).next()!!
        }
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
                    if (s.second) o1.getValue(name).compareTo(o2.getValue(name))
                    else o2.getValue(name).compareTo(o1.getValue(name))
                } else a
            }
        }

        Collections.sort(result, compare)

        return (if (skip > 0 && limit != -1L) result.subList (skip.toInt(), skip.toInt() + limit.toInt())
        else if (skip > 0) result.subList (skip.toInt(), result.size)
        else if (limit != -1L) result.subList (0, limit.toInt())
        else result)
    }
}
