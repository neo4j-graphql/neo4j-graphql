package org.neo4j.graphql

import graphql.language.*
import graphql.schema.GraphQLList
import graphql.schema.GraphQLNonNull
import graphql.schema.GraphQLType

fun Value.extract(): Any =
        when (this) {
            is ObjectValue -> this.objectFields.associate { it.name to it.value.extract() }
            is IntValue -> this.value.toLong()
            is FloatValue -> this.value.toDouble()
            is BooleanValue -> this.isValue
            is StringValue -> this.value
            is EnumValue -> this.name
            is VariableReference -> "{`${this.name}`}" // todo $name
            is ArrayValue -> this.values.map { it.extract() }.toList()
            else -> throw IllegalArgumentException("Unknown Value $this ${this.javaClass}")
        }
fun Field.cypher() = this.getDirective("cypher")?.let { Pair(it.getArgument("statement").value.extract(), it.getArgument("params")?.value?.extract()) }

data class CypherDefinition(val statement:String, val  params:Map<String,Any>? = emptyMap(), val passThrough:Boolean = false)
fun FieldDefinition.cypher() : CypherDefinition ?=
        this.getDirective("cypher")?.let {
            CypherDefinition(it.getArgument("statement").value.extract().toString(),
                    it.getArgument("params")?.value?.extract() as Map<String,Any>?,
                    (it.getArgument("passThrough")?.value?.extract() ?: false) as Boolean)
        }

fun Node.description() : String? = this.comments.let {  if (it.isEmpty()) null else it.map { it.content }.joinToString(" ").trim() }

fun Type.inner() : String = when (this) {
    is ListType -> this.type.inner()
    is NonNullType -> this.type.inner()
    is TypeName -> this.name
    else -> "unknown type $this"
}
fun GraphQLType.inner() : GraphQLType = when (this) {
    is GraphQLList -> this.wrappedType.inner()
    is GraphQLNonNull -> this.wrappedType.inner()
    else -> this
}
fun GraphQLType.isList() : Boolean = when (this) {
    is GraphQLList -> true
    is GraphQLNonNull -> this.wrappedType.isList()
    else -> false
}
