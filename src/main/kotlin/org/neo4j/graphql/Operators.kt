package org.neo4j.graphql

import graphql.Scalars
import graphql.schema.GraphQLEnumType
import graphql.schema.GraphQLInputType
import graphql.schema.GraphQLObjectType
import graphql.schema.GraphQLTypeReference

enum class Operators(val suffix:String, val op:String, val not :Boolean = false) {
    EQ("","="),
    IS_NULL("", ""),
    IS_NOT_NULL("", "", true),
    NEQ("not","=", true),
    GTE("gte",">="),
    GT("gt",">"),
    LTE("lte","<="),
    LT("lt","<"),

    NIN("not_in","IN", true),
    IN("in","IN"),
    NC("not_contains","CONTAINS", true),
    NSW("not_starts_with","STARTS WITH", true),
    NEW("not_ends_with","ENDS WITH", true),
    C("contains","CONTAINS"),
    SW("starts_with","STARTS WITH"),
    EW("ends_with","ENDS WITH"),

    SOME("some","ANY"),
    NONE("none","NONE"),
    ALL("every","ALL"),
    SINGLE("single","SINGLE")
    ;

    val list = op == "IN"

    companion object {
        val ops = enumValues<Operators>().sortedWith(Comparator.comparingInt<Operators> { it.suffix.length }).reversed()
        val allNames = ops.map { it.suffix }
        val allOps = ops.map { it.op }

        fun resolve(field: String, value: Any?) : Pair<String, Operators> {
            val fieldOperator = ops.find { field.endsWith("_" + it.suffix) }
            val unaryOperator = if (value is UnaryOperator) unaryOperatorOf(field, value) else Operators.EQ
            val op = fieldOperator ?: unaryOperator
            val name = if (op.suffix.isEmpty()) field else field.substring(0, field.length - op.suffix.length - 1)
            return name to op
        }

        private fun unaryOperatorOf(field: String, value: Any?): Operators =
            when (value) {
                is IsNullOperator -> if (field.endsWith("_not")) IS_NOT_NULL else IS_NULL
                else -> throw IllegalArgumentException("Unknown unary operator $value")
            }

        fun forType(type: GraphQLInputType) : List<Operators> =
                if (type == Scalars.GraphQLBoolean) listOf(EQ, NEQ)
                else if (type is GraphQLEnumType || type is GraphQLObjectType || type is GraphQLTypeReference) listOf(EQ, NEQ, IN, NIN)
                else listOf(EQ, NEQ, IN, NIN,LT,LTE,GT,GTE) +
                        if (type == Scalars.GraphQLString || type == Scalars.GraphQLID) listOf(C,NC, SW, NSW,EW,NEW) else emptyList()

    }

    fun fieldName(fieldName: String) = if (this == EQ) fieldName else fieldName + "_" + suffix
}
