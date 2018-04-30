package org.neo4j.graphql

import graphql.language.*
import org.neo4j.kernel.internal.Version

abstract class CypherGenerator {
    companion object {
        val VERSION = Version.getNeo4jVersion()
        val DEFAULT_CYPHER_VERSION = "3.2"

        fun instance(): CypherGenerator {
            return Cypher31Generator()
        }
        fun attr(variable: String, field: String) = "`$variable`.`$field`"

        fun isPlural(name: String) = name.endsWith("s")

        fun singular(name: String) = name.substring(0, name.length - 1)

        fun formatAnyValue(value: Any?): String =
                when (value) {
                    null -> "null"
                    is String -> "\"${value}\""
                    is Map<*, *> -> "{" + value.map { it.key.toString() + ":" + formatAnyValue(it.value) }.joinToString(",") + "}"
                    is Iterable<*> -> "[" + value.map { formatAnyValue(it) }.joinToString(",") + "]"
                    else -> value.toString()
                }

        fun formatValue(value: Value?): String =
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
    abstract fun generateQueryForField(field: Field, fieldDefinition: FieldDefinition? = null, isMutation: Boolean = false,
                                       fragments: Map<String, FragmentDefinition> = emptyMap(), params: Map<String, Any> = emptyMap()): String
}

class Cypher31Generator : CypherGenerator() {
    fun projectMap(field: Field, variable: String, md: MetaData, ctx: GeneratorContext): String {
        val selectionSet = field.selectionSet ?: return ""

        return projectSelectionFields(md, variable, selectionSet, ctx).map{
            val array = md.properties[it.first]?.type?.array ?: false
            // todo fix handling of primitive arrays in graphql-java
            if (array) {
                "`${it.first}` : [x IN ${it.second} | x]"
            } else {
                if (it.second == attr(variable, it.first)) ".`${it.first}`"
                else "`${it.first}` : ${it.second}"
            }
        }.joinNonEmpty(", ","`$variable` {","}")
    }


    interface Predicate {
        fun toExpression(variable:String) : String
    }

    data class CompoundPredicate(val parts : List<Predicate>, val op : String = "AND") : Predicate {
        override fun toExpression(variable: String) = parts.map { it.toExpression(variable) }.joinNonEmpty(" "+op+" ","(",")")
    }

    data class ExpressionPredicate(val name:String, val op: Operators, val value:Any?) : Predicate {
        val not = if (op.not) "NOT" else ""
        override fun toExpression(variable:String) =  "$not `${variable}`.`$name` ${op.op} ${formatAnyValue(value)}"
    }

    data class RelationPredicate(val name: String, val op: Operators, val value: Map<*,*>, val md: MetaData) : Predicate {
        val not = if (op.not) "NOT" else ""
        // (md)-[:TYPE]->(related) | pred] = 0/1/ > 0 | =
        // ALL/ANY/NONE/SINGLE(p in (md)-[:TYPE]->() WHERE pred(last(nodes(p)))
        // ALL/ANY/NONE/SINGLE(x IN [(md)-[:TYPE]->(o) | pred(o)] WHERE x)

        override fun toExpression(variable:String) : String {
            val prefix = when (op) {
                Operators.EQ -> "ALL"
                Operators.NEQ -> "ALL" // bc of not
                else -> op.op
            }
            val rel = md.relationshipFor(name)!!
            val (left,right) = if (rel.out) "" to ">" else "<" to ""
            val other = variable+"_"+rel.label
            val pred = CompoundPredicate(value.map { it -> val (field,op)=Operators.resolve(it.key.toString());ExpressionPredicate(field, op, it.value) }).toExpression(other)
            return "$not $prefix(x IN [(`$variable`)$left-[:`${rel.type}`]-$right(`$other`) | $pred] WHERE x)"
        }
    }

    fun where(field: Field, variable: String, md: MetaData, orderBys: MutableList<Pair<String,Boolean>>, parameters: Map<String, Any>): String {
        val filterPredicates = mutableListOf<Predicate>()
        val predicates = field.arguments.mapNotNull {
            val argName = it.name
            val argValue = it.value
            val value : Any? = if (argValue is VariableReference) parameters[argValue.name] else argValue.extract()
            when (argName) {
                "filter" -> {
                    if (value is Map<*,*>) filterPredicates.add(CompoundPredicate(value.map { (k,v) -> toExpression(k.toString(), v, md) }, "AND"))
                    null
                }
                "orderBy" -> {
                    extractOrderByEnum(it, orderBys, parameters)
                    null
                }
                GraphQLSchemaBuilder.ArgumentProperties.NodeId.name -> GraphQLSchemaBuilder.ArgumentProperties.NodeId.argument(variable,field.name, value)
                GraphQLSchemaBuilder.ArgumentProperties.NodeIds.name -> GraphQLSchemaBuilder.ArgumentProperties.NodeIds.argument(variable,field.name,value)
                "first" -> null
                "offset" -> null
                else -> {
                    if (isPlural(argName) && value is Iterable<*> && md.properties.containsKey(singular(argName)))
                        "`${variable}`.`${singular(argName)}` IN ${formatAnyValue(value)}"
                    else
                        "`${variable}`.`$argName` = ${formatAnyValue(value)}"
                }
            // todo directives for more complex filtering
        }}
        return if (predicates.isEmpty() && filterPredicates.isEmpty()) "" else "WHERE " + (predicates + filterPredicates.map { it.toExpression(variable) }).joinToString("\nAND ")
    }

    private fun toExpression(name: String, value: Any?, md: MetaData): Predicate =
            if (name == "AND" || name == "OR")
                if (value is Iterable<*>) {
                    CompoundPredicate(value.map { toExpression("AND", it, md) }, name)
                } else if (value is Map<*,*>){
                    CompoundPredicate(value.map { (k,v) -> toExpression(k.toString(), v, md) }, name)
                } else {
                    throw IllegalArgumentException("Unexpected value for filter: $value")
                }
            else {
                val (fieldName, op) = Operators.resolve(name)
                if (md.hasRelationship(fieldName)) {
                    if (value is Map<*,*>) RelationPredicate(fieldName,op,value, md)
                    else throw IllegalArgumentException("Input for $fieldName must be an filter-InputType")
                } else {
                    ExpressionPredicate(fieldName, op, value)
                }
            }

    private fun extractOrderByEnum(argument: Argument, orderBys: MutableList<Pair<String, Boolean>>, parameters: Map<String, Any>) {
        fun extractSortFields(name: String) : Unit {
            if (name.endsWith("_desc")) {
                orderBys.add(Pair(name.substring(0,name.lastIndexOf("_")), false))
            }
            if (name.endsWith("_asc")) {
                orderBys.add(Pair(name.substring(0,name.lastIndexOf("_")), true))
            }
        }

        val value = argument.value
        if (value is VariableReference) {
            val values = parameters.get(value.name)
            when (values) {
                is List<*> -> values.forEach{extractSortFields(it.toString())}
                is String -> extractSortFields(values)
            }
        }
        if (value is EnumValue) {
            extractSortFields(value.name)
        }
        if (value is ArrayValue) {
            value.values.filterIsInstance<EnumValue>().forEach{extractSortFields(it.name)}
        }
    }

    fun projectSelectionFields(md: MetaData, variable: String, selectionSet: SelectionSet, ctx: GeneratorContext): List<Pair<String, String>> {
        return listOf(Pair("_labels", "graphql.labels(`$variable`)")) +
                projectFragments(md, variable, selectionSet.selections, ctx) +
                projectNamedFragments(md, variable, selectionSet.selections, ctx) +
                selectionSet.selections.filterIsInstance<Field>().mapNotNull { projectField(it, md, variable, ctx) }
    }

    fun projectFragments(md: MetaData, variable: String, selections: MutableList<Selection>, ctx: GeneratorContext): List<Pair<String, String>> {
        return selections.filterIsInstance<InlineFragment>().flatMap {
            val fragmentTypeName = it.typeCondition.name
            val fragmentMetaData = GraphSchemaScanner.getMetaData(fragmentTypeName)!!
            if (fragmentMetaData.labels.contains(md.type) || fragmentMetaData.type == md.type) {
                // these are the nested fields of the fragment
                // it could be that we have to adapt the variable name too, and perhaps add some kind of rename
                it.selectionSet.selections.filterIsInstance<Field>().map { projectField(it, fragmentMetaData, variable, ctx) }.filterNotNull()
            } else {
                emptyList<Pair<String, String>>()
            }
        }
    }
    fun projectNamedFragments(md: MetaData, variable: String, selections: MutableList<Selection>, ctx: GeneratorContext): List<Pair<String, String>> {
        return selections.filterIsInstance<FragmentSpread>().flatMap {
            ctx.fragment(it.name)?.let {
                val fragmentTypeName = it.typeCondition.name
                val fragmentMetaData = GraphSchemaScanner.getMetaData(fragmentTypeName)!!
                if (fragmentMetaData.labels.contains(md.type) || fragmentMetaData.type == md.type) {
                    // these are the nested fields of the fragment
                    // it could be that we have to adapt the variable name too, and perhaps add some kind of rename
                    it.selectionSet.selections.filterIsInstance<Field>().map { projectField(it, fragmentMetaData, variable, ctx) }.filterNotNull()
                } else {
                    emptyList<Pair<String, String>>()
                }
            }?: emptyList<Pair<String, String>>()
        }
    }


    private fun projectField(f: Field, md: MetaData, variable: String, ctx: GeneratorContext): Pair<String, String>? {
        val field = f.name

        val cypherStatement = md.cypherFor(field)
        val relationship = md.relationshipFor(field) // todo correct medatadata of

        val expectMultipleValues = md.properties[field]?.type?.array ?: true

        return if (!cypherStatement.isNullOrEmpty()) {

            val arguments = f.arguments.associate { it.name to it.value.extract() }
                    .mapValues { if (it.value is String) "\"${it.value}\"" else it.value.toString() }

            val params = (mapOf("this" to "`$variable`") + arguments).entries
                    .joinToString(",", "{", "}") { "`${it.key}`:${it.value}" }

            val prefix  = if (!cypherStatement!!.contains(Regex("this\\s*\\}?\\s+AS\\s+",RegexOption.IGNORE_CASE))) "WITH {this} AS this " else ""
            val cypherFragment = "graphql.run('${prefix}${cypherStatement}', $params, $expectMultipleValues)"

            if (relationship != null) {
                val (patternComp, _) = formatCypherDirectivePatternComprehension(md, cypherFragment, f, ctx.copy(orderBys = mutableListOf()))
                Pair(field, if (relationship.multi) patternComp else "head(${patternComp})")
            } else {
                Pair(field, cypherFragment) // TODO escape cypher statement quotes
            }
        } else {
            if (relationship == null) {
                if (GraphQLSchemaBuilder.ArgumentProperties.NodeId.matches(field)) GraphQLSchemaBuilder.ArgumentProperties.NodeId.render(variable)
                else Pair(field, attr(variable, field))
            } else {
                if (f.selectionSet == null) null // todo
                else {
                    val (patternComp, _) = formatPatternComprehension(md, variable, f, ctx.copy(orderBys = mutableListOf())) // metaData(info.label)
                    Pair(field, if (relationship.multi) patternComp else "head(${patternComp})")
                }
            }
        }
    }

    fun nestedPatterns(metaData: MetaData, variable: String, selectionSet: SelectionSet, ctx: GeneratorContext): String {
        return projectSelectionFields(metaData, variable, selectionSet, ctx).map{ pair ->
            val (fieldName, projection) = pair
            "$projection AS `$fieldName`"
        }.joinToString(",\n","RETURN ")
    }

    fun formatCypherDirectivePatternComprehension(md: MetaData, cypherFragment: String, field: Field, ctx: GeneratorContext): Pair<String,String> {
        val fieldName = field.name
        val info = md.relationshipFor(fieldName) ?: return Pair("","")
        val fieldMetaData = GraphSchemaScanner.getMetaData(info.label)!!

        val pattern = "x IN $cypherFragment"

        val projection = projectMap(field, "x", fieldMetaData, ctx)
        val result = "[ $pattern | $projection ]"
        val skipLimit = skipLimit(field)
        return Pair(result + subscript(skipLimit), "x")
    }

    private fun subscriptInt(skipLimit: Pair<Number?, Number?>): String {
        if (skipLimit.first == null && skipLimit.second == null) return ""

        val skip = skipLimit.first?.toInt() ?: 0
        val limit = if (skipLimit.second == null) -1 else skip + (skipLimit.second?.toInt() ?: 0)
        return "[$skip..$limit]"
    }
    private fun subscript(skipLimit: Pair<String?, String?>): String {
        if (skipLimit.first == null && skipLimit.second == null) return ""

        val skip = skipLimit.first ?: "0"
        val limit = if (skipLimit.second == null) "-1" else skip + "+" + (skipLimit.second ?: "0")
        return "[$skip..$limit]"
    }

    fun formatPatternComprehension(md: MetaData, variable: String, field: Field, ctx: GeneratorContext): Pair<String,String> {
        val fieldName = field.name
        val info = md.relationshipFor(fieldName) ?: return Pair("","")
        val fieldVariable = variable + "_" + fieldName

        val arrowLeft = if (!info.out) "<" else ""
        val arrowRight = if (info.out) ">" else ""

        val fieldMetaData = GraphSchemaScanner.getMetaData(info.label)!!

        val pattern = "(`$variable`)$arrowLeft-[:`${info.type}`]-$arrowRight(`$fieldVariable`:`${info.label}`)"
        val orderBys2 = mutableListOf<Pair<String,Boolean>>()
        val where = where(field, fieldVariable, fieldMetaData, orderBys2, parameters = ctx.params)
        val projection = projectMap(field, fieldVariable, fieldMetaData, ctx) // [x IN graph.run ... | x {.name, .age } ] as recommendedMovie if it's a relationship/entity Person / Movie
        var result = "[ $pattern $where | $projection]"
        // todo parameters, use subscripts instead
        val skipLimit = skipLimit(field)
        if (orderBys2.isNotEmpty()) {
            val orderByParams = orderBys2.map { "'${if (it.second) "^" else ""}${it.first}'" }.joinToString(",", "[", "]")
            result = "graphql.sortColl($result,$orderByParams)"
        }
        return Pair(result + subscript(skipLimit),fieldVariable)
    }

    data class GeneratorContext(val orderBys: MutableList<Pair<String,Boolean>> = mutableListOf(),
                                val fragments: Map<String,FragmentDefinition>,
                                val metaDatas:Map<String,MetaData>,
                                val params : Map<String,Any> = emptyMap()) {
        fun metaData(name: String) = metaDatas.get(name)
        fun fragment(name: String) = fragments.get(name)
    }
    override fun generateQueryForField(field: Field, fieldDefinition: FieldDefinition?, isMutation: Boolean, fragments: Map<String, FragmentDefinition>, params: Map<String, Any>): String {
        val ctx = GeneratorContext(fragments = fragments, metaDatas = GraphSchemaScanner.allTypes(), params = params)
        val name = field.name
        val typeName = fieldDefinition?.type?.inner() ?: "no field definition"
        val md = ctx.metaData(name) ?: ctx.metaData(typeName) ?: throw IllegalArgumentException("Cannot resolve as type $name or $typeName")
        val variable = md.type
        val orderBys = mutableListOf<Pair<String,Boolean>>()
        val procedure = if (isMutation) "updateForNodes" else "queryForNodes"
        val cypherDefinition = fieldDefinition?.cypher()
        val isDynamic = cypherDefinition != null
        val query = cypherDefinition?.
                let {
                    val passedInParams = field.arguments.map { "`${it.name}` : {`${it.name}`}" }.joinToString(",", "{", "}")
                    """CALL graphql.$procedure("${it.statement}",${passedInParams}) YIELD node AS `$variable`"""
                }
                ?: "MATCH (`$variable`:`$name`)"

        val projectFields = projectSelectionFields(md, variable, field.selectionSet, ctx)
        val resultProjection = projectFields.map { pair ->
            val (fieldName, projection) = pair
            // todo fix handling of primitive arrays in graphql-java
            val type = md.properties[fieldName]?.type
            if (type?.array == true && md.cypherFor(fieldName) == null) {
                "[x IN $projection |x] AS `$fieldName`"
            } else {
                "$projection AS `$fieldName`"
            }
        }.joinToString(",\n","RETURN ")

        val resultFieldNames = projectFields.map { it.first }.toSet()
        val where = if (isDynamic) "" else where(field, variable, md, orderBys, params)
        val parts = listOf(
                query,
                where,
                resultProjection,
                // todo check if result is in returned projections
                orderBys.map { (if (!resultFieldNames.contains(it.first))  "`$variable`." else "") + "`${it.first}` ${if (it.second) "asc" else "desc"}" }.joinNonEmpty(",", "ORDER BY ")
        ) +  skipLimitStatements(skipLimit(field))

        val statement = parts.filter { !it.isNullOrEmpty() }.joinToString("\n")
        return statement
    }

    private fun cypherDirective(field: Field): Directive? =
            field.directives.filter { it.name == "cypher" }.firstOrNull()

    private fun skipLimitStatements(skipLimit: Pair<String?, String?>) =
            listOf<String?>( skipLimit.first?.let { "SKIP $it" },skipLimit.second?.let { "LIMIT $it" })

    private fun skipLimitInt(field: Field): Pair<Number?,Number?> = Pair(
            intValue(argumentByName(field, "offset")),
            intValue(argumentByName(field, "first")))

    private fun skipLimit(field: Field): Pair<String?,String?> = Pair(
            argumentValueOrParam(field, "offset"), argumentValueOrParam(field, "first"))

    private fun argumentByName(field: Field, name: String) = field.arguments.firstOrNull { it.name == name }

    private fun argumentValueOrParam(field: Field, name: String)
            = field.arguments
            .filter { it.name == name }
            .map { it.value }
            // todo variables seem to be automatically resolved to field name variables
            .map { if (it is VariableReference) "{${name}}" else valueAsString(it) }
            .firstOrNull()

    private fun valueAsString(it: Value?): String? = when (it) {
        is IntValue -> it.value.toString()
        is FloatValue -> it.value.toString()
        is StringValue -> "'" + it.value + "'"
        is EnumValue -> "'" + it.name + "'"
        is BooleanValue -> it.isValue.toString()
        is ArrayValue -> it.values.map { valueAsString(it) }.joinToString(",","[","]")
        is ObjectValue -> it.objectFields.map { "`${it.name}`:${valueAsString(it.value)}" }.joinToString(",","{","}")
        is VariableReference -> "{${it.name}}"
        else -> null
    }

    private fun intValue(it: Argument?) : Number? {
        val value = it?.value
        return if (value is IntValue) value.value
        else null
    }
}
