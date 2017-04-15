package org.neo4j.graphql

import graphql.language.*
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.internal.Version

abstract class CypherGenerator {
    companion object {
        val VERSION = Version.getNeo4jVersion()
        val DEFAULT_CYPHER_VERSION = "3.1"

        fun instance(db: GraphDatabaseService) : CypherGenerator {
            try {
                // val first = db.execute("CYPHER 3.1 RETURN true as version").columnAs<Boolean>("version").next()
                if (VERSION.startsWith("3.1")) return Cypher31Generator()
            } catch (e: Exception) {
                e.printStackTrace()
            }
            return Cypher30Generator()
        }
    }

    abstract fun compiled() : String

    fun generateQueryForField(field: Field): String {
        val name = field.name
        val variable = name
        val md = metaData(name)
        val orderBys = mutableListOf<String>()
        return "MATCH (`$variable`:`$name`) \n" +
                where(field, variable, md, orderBys) +
                optionalMatches(md, variable, field.selectionSet.selections, orderBys) +
                // TODO order within each
                orderBy(orderBys) +
                "\nRETURN " + projection(field, variable, md)

    }

    protected fun orderBy(orderBys:List<String>) = if (orderBys.isEmpty()) "" else orderBys.joinToString(",", "\nWITH * ORDER BY ")

    open protected fun optionalMatches(metaData: MetaData, variable: String, selections: Iterable<Selection>, orderBys: MutableList<String>) =
            selections.map {
                when (it) {
                    is Field -> formatNestedRelationshipMatch(metaData, variable, it, orderBys)
                    else -> ""
                }
            }.joinToString("\n")

    protected fun attr(variable: String, field: String) = "`$variable`.`$field`"

    open protected fun formatNestedRelationshipMatch(md: MetaData, variable: String, field: Field, orderBys: MutableList<String>): String {
        val fieldName = field.name
        val info = md.relationshipFor(fieldName) ?: return ""
        val fieldVariable = variable + "_" + fieldName;

        val arrowLeft = if (!info.out) "<" else ""
        val arrowRight = if (info.out) ">" else ""

        val fieldMetaData = GraphSchemaScanner.getMetaData(info.label)!!

        val pattern = "(`$variable`)$arrowLeft-[:`${info.type}`]-$arrowRight(`$fieldVariable`:`${info.label}`)"
        val where = where(field, fieldVariable, fieldMetaData, orderBys)

        val result = "\nOPTIONAL MATCH $pattern $where\n"
        return result +
                if (field.selectionSet.selections.isNotEmpty())
                    optionalMatches(fieldMetaData, fieldVariable, field.selectionSet.selections, orderBys)
                else ""
    }

    protected fun metaData(name: String) = GraphSchemaScanner.getMetaData(name)!!

    protected fun where(field: Field, variable: String, md: MetaData, orderBys: MutableList<String>): String {
        val predicates = field.arguments.mapNotNull {
            val name = it.name
            if (name == "orderBy") {
                if (it.value is ArrayValue) {
                    (it.value as ArrayValue).values.filterIsInstance<EnumValue>().forEach {
                        val pairs = it.name.split("_")
                        orderBys.add("`$variable`.`${pairs[0]}` ${pairs[1]}")
                    }
                }
                null
            }
            else
                if (isPlural(name) && it.value is ArrayValue && md.properties.containsKey(singular(name)))
                    "`${variable}`.`${singular(name)}` IN ${formatValue(it.value)} "
                else
                    "`${variable}`.`$name` = ${formatValue(it.value)} "
            // todo directives for more complex filtering
        }.joinToString(" AND \n")
        return if (predicates.isBlank()) "" else " WHERE " + predicates;
    }

    private fun isPlural(name: String) = name.endsWith("s")

    private fun singular(name: String) = name.substring(0, name.length - 1)

    private fun formatValue(value: Value?): String =
            when (value) {
                is VariableReference -> "{`${value.name}`}"
            // todo turn into parameters  !!
                is IntValue -> value.value.toString()
                is FloatValue -> value.value.toString()
                is BooleanValue -> value.isValue().toString()
                is StringValue -> "\"${value.value}\""
                is EnumValue -> "\"${value.name}\""
                is ObjectValue -> "{" + value.objectFields.map { it.name + ":" + formatValue(it.value) }.joinToString(",") + "}"
                is ArrayValue -> "[" + value.values.map { formatValue(it) }.joinToString(",") + "]"
                else -> "" // todo raise exception ?
            }

    // todo levels top-down and then reverse, pass in a level parameter that controls at which level values are aggregated / collected
    // if there is nothing at that level just return null or an empty string to abort or compute max-depth upfront (perhaps best to be explicit)
    /*
    MATCH (`Person`:`Person`)


OPTIONAL MATCH (`Person`)-[:`ACTED_IN`]->(`Person_ACTED_IN_Movie`:`Movie`)

OPTIONAL MATCH (`Person_ACTED_IN_Movie`)<-[:`ACTED_IN`]-(`Person_ACTED_IN_Movie_Person_ACTED_IN`:`Person`)

RETURN `Person`.`name` AS `name`, collect(CASE `Person_ACTED_IN_Movie` WHEN null THEN null ELSE {`title` : `Person_ACTED_IN_Movie`.`title`, `Person_ACTED_IN` : collect(CASE `Person_ACTED_IN_Movie_Person_ACTED_IN` WHEN null THEN null ELSE {`name` : `Person_ACTED_IN_Movie_Person_ACTED_IN`.`name`} END)} END) AS `ACTED_IN_Movie`
     */
    open protected fun projection(field: Field, variable: String, md: MetaData): String {
        val selectionSet = field.selectionSet ?: return ""

        return projectSelectionFields(md, variable, selectionSet).map{ "${it.second} AS `${it.first}`" }.joinToString(", ");
    }

    open protected fun projectMap(field: Field, variable: String, md: MetaData): String {
        val selectionSet = field.selectionSet ?: return ""

        return "CASE `$variable` WHEN null THEN null ELSE {"+projectSelectionFields(md, variable, selectionSet).map{ "`${it.first}` : ${it.second}" }.joinToString(", ")+"} END";
    }

    open protected fun projectSelectionFields(md: MetaData, variable: String, selectionSet: SelectionSet): List<Pair<String, String>> {
        return selectionSet.selections.filterIsInstance<Field>().map { f ->
//            val alias = f.alias ?: f.name // alias is handled in graphql layer
            val alias = f.name
            val info = md.relationshipFor(f.name) // todo correct medatadata of

            if (info == null) {
                Pair(alias, "`$variable`.`${f.name}`")
            } else {
                if (f.selectionSet == null) null // todo
                else {
                    val fieldVariable = variable + "_" + f.name
                    val map = projectMap(f, fieldVariable, metaData(info.label))
                    Pair(alias, if (info.multi) "collect($map)" else map)
                }
            }
        }.filterNotNull()
    }

}
class Cypher31Generator : CypherGenerator() {
    override fun compiled() = "compiledExperimentalFeatureNotSupportedForProductionUse"


    override fun projectMap(field: Field, variable: String, md: MetaData): String {
        val selectionSet = field.selectionSet ?: return ""

        return "`$variable` {"+projectSelectionFields(md, variable, selectionSet).map{
            if (it.second == attr(variable, it.first)) ".`${it.first}`"
            else "`${it.first}` : ${it.second}"
        }.joinToString(", ")+"}";
    }

    override fun projectSelectionFields(md: MetaData, variable: String, selectionSet: SelectionSet): List<Pair<String, String>> {
        return selectionSet.selections.filterIsInstance<Field>().map { f ->
//            val alias = f.alias ?: f.name // alias is handled in graphql layer
            val field = f.name
            val info = md.relationshipFor(field) // todo correct medatadata of

            if (info == null) {
                Pair(field, attr(variable, field))
            } else {
                if (f.selectionSet == null) null // todo
                else {
                    val fieldVariable = variable + "_" + field
                    val map = projectMap(f, fieldVariable, metaData(info.label))
                    Pair(field, if (info.multi) "collect($map)" else map)
                }
            }
        }.filterNotNull()
    }

    override protected fun formatNestedRelationshipMatch(md: MetaData, variable: String, field: Field, orderBys: MutableList<String>): String {
        val fieldName = field.name
        val info = md.relationshipFor(fieldName) ?: return ""
        val fieldVariable = variable + "_" + fieldName;

        val arrowLeft = if (!info.out) "<" else ""
        val arrowRight = if (info.out) ">" else ""

        val fieldMetaData = GraphSchemaScanner.getMetaData(info.label)!!

        val pattern = "(`$variable`)$arrowLeft-[:`${info.type}`]-$arrowRight(`$fieldVariable`:`${info.label}`)"
        val where = where(field, fieldVariable, fieldMetaData, orderBys)

        val result = "\nOPTIONAL MATCH $pattern $where\n"
        return result +
                if (field.selectionSet.selections.isNotEmpty())
                    optionalMatches(fieldMetaData, fieldVariable, field.selectionSet.selections, orderBys)
                else ""
    }

}
class Cypher30Generator : CypherGenerator() {
    override fun compiled() = "compiled"
}
