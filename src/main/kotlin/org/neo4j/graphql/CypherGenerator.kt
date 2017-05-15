package org.neo4j.graphql

import graphql.language.*
import org.neo4j.kernel.internal.Version

fun <T> Iterable<T>.joinNonEmpty(separator: CharSequence = ", ", prefix: CharSequence = "", postfix: CharSequence = "", limit: Int = -1, truncated: CharSequence = "...", transform: ((T) -> CharSequence)? = null): String {
    return if (iterator().hasNext()) joinTo(StringBuilder(), separator, prefix, postfix, limit, truncated, transform).toString() else ""
}

abstract class CypherGenerator {
    companion object {
        val VERSION = Version.getNeo4jVersion()
        val DEFAULT_CYPHER_VERSION = "3.1"

        fun instance(): CypherGenerator {
            if (VERSION.startsWith("3.1")) return Cypher31Generator()
            return Cypher30Generator()
        }
    }

    abstract fun compiled() : String

    abstract fun generateQueryForField(field: Field): String

    protected fun metaData(name: String) = GraphSchemaScanner.getMetaData(name)!!

    protected fun attr(variable: String, field: String) = "`$variable`.`$field`"

    protected fun isPlural(name: String) = name.endsWith("s")

    protected fun singular(name: String) = name.substring(0, name.length - 1)

    protected fun formatValue(value: Value?): String =
            when (value) {
                is VariableReference -> "{`${value.name}`}"
            // todo turn into parameters  !!
                is IntValue -> value.value.toString()
                is FloatValue -> value.value.toString()
                is BooleanValue -> value.isValue.toString()
                is StringValue -> "\"${value.value}\""
                is EnumValue -> "\"${value.name}\""
                is ObjectValue -> "{" + value.objectFields.map { it.name + ":" + formatValue(it.value) }.joinToString(",") + "}"
                is ArrayValue -> "[" + value.values.map { formatValue(it) }.joinToString(",") + "]"
                else -> "" // todo raise exception ?
            }
}

class Cypher31Generator : CypherGenerator() {
    override fun compiled() = "compiledExperimentalFeatureNotSupportedForProductionUse"


    fun projectMap(field: Field, variable: String, md: MetaData, orderBys: MutableList<Pair<String,Boolean>>): String {
        val selectionSet = field.selectionSet ?: return ""

        return projectSelectionFields(md, variable, selectionSet, orderBys).map{
            if (it.second == attr(variable, it.first)) ".`${it.first}`"
            else "`${it.first}` : ${it.second}"
        }.joinNonEmpty(", ","`$variable` {","}")
    }

    fun where(field: Field, variable: String, md: MetaData, orderBys: MutableList<Pair<String,Boolean>>): String {
        val predicates = field.arguments.mapNotNull {
            val name = it.name
            when (name) {
                "orderBy" -> {
                    extractOrderByEnum(it, orderBys)
                    null
                }
                "first" -> null
                "offset" -> null
                else -> {
                    if (isPlural(name) && it.value is ArrayValue && md.properties.containsKey(singular(name)))
                        "`${variable}`.`${singular(name)}` IN ${formatValue(it.value)}"
                    else
                        "`${variable}`.`$name` = ${formatValue(it.value)}"
                }
            // todo directives for more complex filtering
        }}.joinToString("\nAND ")
        return if (predicates.isBlank()) "" else "WHERE " + predicates
    }

    private fun extractOrderByEnum(argument: Argument, orderBys: MutableList<Pair<String, Boolean>>) {
        if (argument.value is ArrayValue) {
            (argument.value as ArrayValue).values.filterIsInstance<EnumValue>().forEach {
                val pairs = it.name.split("_")
                orderBys.add(Pair(pairs[0], pairs[1].toLowerCase() == "asc"))
            }
        }
    }

    fun projectSelectionFields(md: MetaData, variable: String, selectionSet: SelectionSet, orderBys: MutableList<Pair<String, Boolean>>): List<Pair<String, String>> {
        return listOf(Pair("_labels", "labels(`$variable`)")) +
                projectFragments(md, variable, selectionSet.selections, orderBys) +
                selectionSet.selections.filterIsInstance<Field>().mapNotNull { projectField(it, md, variable, orderBys) }
    }

    fun projectFragments(md: MetaData, variable: String, selections: MutableList<Selection>, orderBys: MutableList<Pair<String, Boolean>>): List<Pair<String, String>> {
        return selections.filterIsInstance<InlineFragment>().flatMap {
            val fragmentTypeName = it.typeCondition.name
            val fragmentMetaData = GraphSchemaScanner.getMetaData(fragmentTypeName)!!
            if (fragmentMetaData.labels.contains(md.type)) {
                // these are the nested fields of the fragment
                // it could be that we have to adapt the variable name too, and perhaps add some kind of rename
                it.selectionSet.selections.filterIsInstance<Field>().map { projectField(it, fragmentMetaData, variable, orderBys) }.filterNotNull()
            } else {
                emptyList<Pair<String, String>>()
            }
        }
    }


    private fun projectField(f: Field, md: MetaData, variable: String, orderBys: MutableList<Pair<String, Boolean>>): Pair<String, String>? {
        val field = f.name

        val cypherStatement = md.cypherFor(field)
        val relationship = md.relationshipFor(field) // todo correct medatadata of

        val expectMultipleValues = md.properties[field]?.type?.array ?: true

        return if (!cypherStatement.isNullOrEmpty()) {

            val arguments = f.arguments.associate { it.name to it.value.extract() }
                    .mapValues { if (it.value is String) "\"${it.value}\"" else it.value.toString() }

            val params = (mapOf("this" to variable) + arguments).entries
                    .joinToString(",", "{", "}") { "`${it.key}`:${it.value}" }

            val cypherFragment = "graphql.run('$cypherStatement', $params, $expectMultipleValues)"

            if (relationship != null) {
                val (patternComp, _) = formatCypherDirectivePatternComprehension(md, cypherFragment, f)
                Pair(field, if (relationship.multi) patternComp else "head(${patternComp})")
            } else {
                Pair(field, cypherFragment) // TODO escape cypher statement quotes
            }
        } else {
            if (relationship == null) {
                Pair(field, attr(variable, field))
            } else {
                if (f.selectionSet == null) null // todo
                else {
                    val (patternComp, _) = formatPatternComprehension(md, variable, f, orderBys) // metaData(info.label)
                    Pair(field, if (relationship.multi) patternComp else "head(${patternComp})")
                }
            }
        }
    }

    fun nestedPatterns(metaData: MetaData, variable: String, selectionSet: SelectionSet, orderBys: MutableList<Pair<String,Boolean>>): String {
        return projectSelectionFields(metaData, variable, selectionSet, orderBys).map{ pair ->
            val (fieldName, projection) = pair
            "$projection AS `$fieldName`"
        }.joinToString(",\n","RETURN ")
    }

    fun formatCypherDirectivePatternComprehension(md: MetaData, cypherFragment: String, field: Field): Pair<String,String> {
        val fieldName = field.name
        val info = md.relationshipFor(fieldName) ?: return Pair("","")
        val fieldMetaData = GraphSchemaScanner.getMetaData(info.label)!!

        val pattern = "x IN $cypherFragment"

        val projection = projectMap(field, "x", fieldMetaData, mutableListOf<Pair<String, Boolean>>())
        val result = "[ $pattern | $projection ]"
        val skipLimit = skipLimit(field)
        return Pair(result + subscript(skipLimit), "x")
    }

    private fun subscript(skipLimit: Pair<Number?, Number?>): String {
        if (skipLimit.first == null && skipLimit.second == null) return ""

        val skip = skipLimit.first?.toInt() ?: 0
        val limit = if (skipLimit.second == null) -1 else skip + (skipLimit.second?.toInt() ?: 0)
        return "[$skip..$limit]"
    }

    fun formatPatternComprehension(md: MetaData, variable: String, field: Field, orderBysIgnore: MutableList<Pair<String,Boolean>>): Pair<String,String> {
        val fieldName = field.name
        val info = md.relationshipFor(fieldName) ?: return Pair("","")
        val fieldVariable = variable + "_" + fieldName

        val arrowLeft = if (!info.out) "<" else ""
        val arrowRight = if (info.out) ">" else ""

        val fieldMetaData = GraphSchemaScanner.getMetaData(info.label)!!

        val pattern = "(`$variable`)$arrowLeft-[:`${info.type}`]-$arrowRight(`$fieldVariable`:`${info.label}`)"
        val orderBys2 = mutableListOf<Pair<String,Boolean>>()
        val where = where(field, fieldVariable, fieldMetaData, orderBys2)
        val projection = projectMap(field, fieldVariable, fieldMetaData, orderBysIgnore) // [x IN graph.run ... | x {.name, .age } ] as recommendedMovie if it's a relationship/entity Person / Movie
        var result = "[ $pattern $where | $projection]"
        // todo parameters, use subscripts instead
        val skipLimit = skipLimit(field)
        if (orderBys2.isNotEmpty()) {
            val orderByParams = orderBys2.map { "${if (it.second) "^" else ""}'${it.first}'" }.joinToString(",", "[", "]")
            result = "graphql.sortColl($result,$orderByParams)"
        }
        return Pair(result + subscript(skipLimit),fieldVariable)
    }

    override fun generateQueryForField(field: Field): String {
        val name = field.name
        val variable = name
        val md = metaData(name)
        val orderBys = mutableListOf<Pair<String,Boolean>>()

        val parts = listOf(
                "MATCH (`$variable`:`$name`)",
                where(field, variable, md, orderBys),
                nestedPatterns(md, variable, field.selectionSet, orderBys),
                orderBys.map { "${it.first} ${if (it.second) "asc" else "desc"}" }.joinNonEmpty(",", "\nORDER BY ")
        ) +  skipLimitStatements(skipLimit(field))

        return parts.filter { !it.isNullOrEmpty() }.joinToString("\n")
    }

    private fun skipLimitStatements(skipLimit: Pair<Number?, Number?>) =
            listOf<String?>( skipLimit.first?.let { "SKIP $it" },skipLimit.second?.let { "LIMIT $it" })

    private fun skipLimit(field: Field): Pair<Number?,Number?> = Pair(
            intValue(argumentByName(field, "offset")),
            intValue(argumentByName(field, "first")))

    private fun argumentByName(field: Field, name: String) = field.arguments.firstOrNull { it.name == name }

    private fun intValue(it: Argument?) : Number? = (it?.value as IntValue?)?.value

}

class Cypher30Generator : CypherGenerator() {
    override fun compiled() = "compiled"

    fun orderBy(orderBys:List<String>) = if (orderBys.isEmpty()) "" else orderBys.joinNonEmpty(",", "\nWITH * ORDER BY ")

    fun optionalMatches(metaData: MetaData, variable: String, selections: Iterable<Selection>, orderBys: MutableList<String>) =
            selections.map {
                when (it) {
                    is Field -> formatNestedRelationshipMatch(metaData, variable, it, orderBys)
                    else -> ""
                }
            }.joinToString("\n")

    fun formatNestedRelationshipMatch(md: MetaData, variable: String, field: Field, orderBys: MutableList<String>): String {
        val fieldName = field.name
        val info = md.relationshipFor(fieldName) ?: return ""
        val fieldVariable = variable + "_" + fieldName

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

    fun where(field: Field, variable: String, md: MetaData, orderBys: MutableList<String>): String {
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
        return if (predicates.isBlank()) "" else " WHERE " + predicates
    }

    fun projection(field: Field, variable: String, md: MetaData): String {
        val selectionSet = field.selectionSet ?: return ""

        return projectSelectionFields(md, variable, selectionSet).map{ "${it.second} AS `${it.first}`" }.joinToString(", ")
    }

    fun projectMap(field: Field, variable: String, md: MetaData): String {
        val selectionSet = field.selectionSet ?: return ""

        return "CASE `$variable` WHEN null THEN null ELSE {"+projectSelectionFields(md, variable, selectionSet).map{ "`${it.first}` : ${it.second}" }.joinToString(", ")+"} END"
    }

    fun projectSelectionFields(md: MetaData, variable: String, selectionSet: SelectionSet): List<Pair<String, String>> {
        val selections = selectionSet.selections

        return projectFragments(md, selections, variable) +
                selections.filterIsInstance<Field>().map{ projectField(md, variable, it) }.filterNotNull()
    }

    fun projectFragments(md: MetaData, selections: MutableList<Selection>, variable: String): List<Pair<String, String>> {
        return selections.filterIsInstance<InlineFragment>().flatMap {
            val fragmentTypeName = it.typeCondition.name
            val metaData = GraphSchemaScanner.getMetaData(fragmentTypeName)!!
            if (metaData.labels.contains(md.type)) {
                // these are the nested fields of the fragment
                val map = it.selectionSet.selections.filterIsInstance<Field>().map { projectField(md, variable, it) }.filterNotNull()
                map
            } else {
                emptyList<Pair<String, String>>()
            }
        }
    }

    private fun projectField(md: MetaData, variable: String, f: Field): Pair<String, String>? {
        val alias = f.name
        val info = md.relationshipFor(f.name) // todo correct medatadata of

        return if (info == null) {
            Pair(alias, "`$variable`.`${f.name}`")
        } else {
            if (f.selectionSet == null) null // todo
            else {
                val fieldVariable = variable + "_" + f.name
                val map = projectMap(f, fieldVariable, metaData(info.label))
                Pair(alias, if (info.multi) "collect($map)" else map)
            }
        }
    }

    override fun generateQueryForField(field: Field): String {
        val name = field.name
        val variable = name
        val md = metaData(name)
        val orderBys = mutableListOf<String>()

        return "MATCH (`$variable`:`$name`) \n" +
                where(field, variable, md, orderBys) +
                optionalMatches(md, variable, field.selectionSet.selections, orderBys) +
                // TODO order within each
                orderBy(orderBys) +
                " \nRETURN " + projection(field, variable, md)
    }



}
