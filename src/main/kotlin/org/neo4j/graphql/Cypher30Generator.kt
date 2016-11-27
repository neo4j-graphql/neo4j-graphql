package org.neo4j.graphql

import graphql.language.*

class Cypher30Generator {

    fun generateQueryForField(field: Field): String {
        val name = field.name
        val variable = name
        val md = metaData(name)
        return "MATCH (`$variable`:`$name`) \n" +
                where(field, variable) +
                optionalMatches(md, variable, field.selectionSet.selections) +
                " RETURN " + projection(field, variable, md)
    }

    private fun optionalMatches(metaData: MetaData, variable: String, selections: Iterable<Selection>) =
            selections.map {
                when (it) {
                    is Field -> formatField(metaData, variable, it)
                    else -> ""
                }
            }.joinToString("\n")

    private fun formatField(md: MetaData, variable: String, field: Field): String {
        val fieldName = field.name
        val info = md.relationshipFor(fieldName) ?: return ""

        val arrowLeft = if (!info.out) "<" else ""
        val arrowRight = if (info.out) ">" else ""
        return " OPTIONAL MATCH (`$variable`)$arrowLeft-[:`${info.type}`]-$arrowRight(`$fieldName`:`${info.label}`) \n"
        // todo handle conditions on related elements after optional match
        //TODO                    query += addWhere(field, variable);
    }

    private fun metaData(name: String) = GraphSchemaScanner.getMetaData(name)!!

    private fun where(field: Field, variable: String): String {
        if (field.arguments.isEmpty()) return ""
        return " WHERE " +
                field.arguments.map { "`${variable}`.`${it.name}` = ${formatValue(it.value)} " }
                        .joinToString(" AND \n")
    }

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

        return "{"+projectSelectionFields(md, variable, selectionSet).map{ "`${it.first}` : ${it.second}" }.joinToString(", ")+"}";
    }

    private fun projectSelectionFields(md: MetaData, variable: String, selectionSet: SelectionSet): List<Pair<String, String>> {
        return selectionSet.selections.filter { it is Field }.map {
            val f = it as Field
//            val alias = f.alias ?: f.name // alias is handled in graphql layer
            val alias = f.name
            val info = md.relationshipFor(f.name) // todo correct medatadata of

            if (info == null) {
                Pair(alias, "`$variable`.`${f.name}`")
            } else {
                if (f.selectionSet == null) null // todo
                else {
                    val map = projectMap(f, f.name, metaData(info.label))
                    Pair(alias, if (info.multi) "collect($map)" else map)
                }
            }
        }.filterNotNull()
    }
}
