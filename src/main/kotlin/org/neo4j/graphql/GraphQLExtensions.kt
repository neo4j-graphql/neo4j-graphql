package org.neo4j.graphql

import graphql.language.*

fun Value.extract(): Any =
        when (this) {
            is ObjectValue -> this.objectFields.associate { it.name to it.value.extract() }
            is IntValue -> this.value.toInt()
            is FloatValue -> this.value.toDouble()
            is BooleanValue -> this.isValue
            is StringValue -> this.value
            is EnumValue -> this.name
            is VariableReference -> this.name
            is ArrayValue -> this.values.map { it.extract() }.toList()
            else -> throw IllegalArgumentException("Unknown Value $this ${this.javaClass}")
        }
