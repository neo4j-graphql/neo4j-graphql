package org.neo4j.graphql

import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.logging.Log
import org.neo4j.procedure.Context
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure
import org.neo4j.procedure.UserFunction
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

        return (if (skip > 0 && limit != -1L) result.subList (skip.toInt(), limit.toInt())
        else if (skip > 0) result.subList (skip.toInt(), result.size)
        else if (limit != -1L) result.subList (0, limit.toInt())
        else result)
    }
}
