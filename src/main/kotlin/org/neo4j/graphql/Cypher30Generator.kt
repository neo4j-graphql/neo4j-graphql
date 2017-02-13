package org.neo4j.graphql

import graphql.language.*
import java.util.*


class Cypher30Generator {

    companion object {
        val COMPILED = "compiledExperimentalFeatureNotSupportedForProductionUse"
        val DEFAULT_CYPHER_VERSION = "3.0"
    }
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

    private fun orderBy(orderBys: MutableList<String>) = if (orderBys.isEmpty()) "" else orderBys.joinToString(",", "\nWITH * ORDER BY ")

    private fun optionalMatches(metaData: MetaData, variable: String, selections: Iterable<Selection>, orderBys: MutableList<String>) =
            selections.map {
                when (it) {
                    is Field -> formatNestedRelationshipMatch(metaData, variable, it, orderBys)
                    else -> ""
                }
            }.joinToString("\n")

    private fun formatNestedRelationshipMatch(md: MetaData, variable: String, field: Field, orderBys: MutableList<String>): String {
        val fieldName = field.name
        val fieldVariable = variable + "_" + fieldName;
        val info = md.relationshipFor(fieldName) ?: return ""

        val arrowLeft = if (!info.out) "<" else ""
        val arrowRight = if (info.out) ">" else ""

        val fieldMetaData = GraphSchemaScanner.getMetaData(info.label)!!

        val result = "\nOPTIONAL MATCH (`$variable`)$arrowLeft-[:`${info.type}`]-$arrowRight(`$fieldVariable`:`${info.label}`)" +
                where(field, fieldVariable, fieldMetaData, orderBys) + "\n"
        return result +
                if (field.selectionSet.selections.isNotEmpty())
                    optionalMatches(fieldMetaData, fieldVariable, field.selectionSet.selections, orderBys)
                else ""
    }

    private fun metaData(name: String) = GraphSchemaScanner.getMetaData(name)!!

    private fun where(field: Field, variable: String, md: MetaData, orderBys: MutableList<String>): String {
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
            };
            else
                if (isPlural(name) && it.value is ArrayValue && md.properties.containsKey(singular(name)))
                    "`${variable}`.`${singular(name)}` IN ${formatValue(it.value)} "
                else
                    "`${variable}`.`$name` = ${formatValue(it.value)} "
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

    private fun projection(field: Field, variable: String, md: MetaData): String {
        val selectionSet = field.selectionSet ?: return ""

        return projectSelectionFields(md, variable, selectionSet).map{ "${it.second} AS `${it.first}`" }.joinToString(", ");
    }

    private fun projectMap(field: Field, variable: String, md: MetaData): String {
        val selectionSet = field.selectionSet ?: return ""

        return "CASE `$variable` WHEN null THEN null ELSE {"+projectSelectionFields(md, variable, selectionSet).map{ "`${it.first}` : ${it.second}" }.joinToString(", ")+"} END";
    }

    private fun projectSelectionFields(md: MetaData, variable: String, selectionSet: SelectionSet): List<Pair<String, String>> {
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
