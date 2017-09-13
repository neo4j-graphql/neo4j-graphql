package org.neo4j.graphql

import graphql.language.*
import graphql.schema.GraphQLList
import graphql.schema.GraphQLNonNull
import graphql.schema.GraphQLType

fun Value.extract(): Any =
        when (this) {
            is ObjectValue -> this.objectFields.associate { it.name to it.value.extract() }
            is IntValue -> this.value.toInt()
            is FloatValue -> this.value.toDouble()
            is BooleanValue -> this.isValue
            is StringValue -> this.value
            is EnumValue -> this.name
            is VariableReference -> "{`${this.name}`}" // todo $name
            is ArrayValue -> this.values.map { it.extract() }.toList()
            else -> throw IllegalArgumentException("Unknown Value $this ${this.javaClass}")
        }
fun Field.cypher() = this.getDirective("cypher")?.let { Pair(it.getArgument("statement").value.extract(), it.getArgument("params")?.value?.extract()) }
fun FieldDefinition.cypher() : Pair<String,Map<String,Any>?>?= this.getDirective("cypher")?.let { Pair(it.getArgument("statement").value.extract().toString(), it.getArgument("params")?.value?.extract() as Map<String,Any>?) }

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
