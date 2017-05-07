package org.neo4j.graphql

import graphql.language.*
import graphql.parser.Parser

object IDLParser {
    fun parse(input: String): Map<String,MetaData> = (Parser().parseDocument(input).definitions.map {
        when (it) {
//            is TypeDefinition -> toMeta(x)
//            is UnionTypeDefinition -> toMeta(x)
//            is EnumTypeDefinition -> toMeta(x)
            is InterfaceTypeDefinition -> toMeta(it)
//            is InputObjectTypeDefinition -> toMeta(x)
//            is ScalarTypeDefinition -> toMeta(x)
            is ObjectTypeDefinition -> toMeta(it)
            else -> {
                println(it.javaClass); null
            }
        }
    }).filterNotNull().associate{ m -> Pair(m.type, m) }

    fun toMeta(definition: TypeDefinition): MetaData {
        val metaData = MetaData(definition.name)
        if (definition is ObjectTypeDefinition) {
            val labels = definition.implements.filterIsInstance<TypeName>().map { it.name };
            metaData.labels.addAll(labels)
        }

        definition.children.forEach { child ->
            when (child) {
                is FieldDefinition -> {
                    val fieldName = child.name
                    val type = typeFromIDL(child.type)

                    if (type.isBasic()) {
                        metaData.addProperty(fieldName, type)
                    } else {
                        val relation = child.directives.filter { it.name == "relation" }.firstOrNull()

                        if (relation == null) {
                            metaData.mergeRelationship(fieldName, fieldName, type.name, out = true, multi = type.array)
                        } else {

                            val typeName = relation.arguments.filter {it.name == "name" }.map { (it.value as StringValue).value}.firstOrNull()?:fieldName
                            val out = relation.arguments.filter {it.name == "direction" }.map { !(it.value as StringValue).value.equals( "IN", ignoreCase = true) }.firstOrNull()?:true

                            metaData.mergeRelationship(typeName, fieldName, type.name, out, type.array)
                        }
                    }
                    if (type.nonNull) {
                        metaData.addIdProperty(fieldName)
                    }
                    child.directives.filter { it.name == "cypher" }.map { (it.arguments[0].value as StringValue).value}.forEach { metaData.addCypher(fieldName, it)}
                }
                is TypeName -> println("TypeName: " + child.name + " " + child.javaClass + " in " + definition.name)
                is EnumValueDefinition -> println("EnumValueDef: " + child.name + " " + child.directives.map { it.name })
                else -> println(child.javaClass.name)
            }
        }
        return metaData
    }

    private fun typeFromIDL(type: Type, given: MetaData.PropertyType = MetaData.PropertyType("String")): MetaData.PropertyType = when (type) {
        is TypeName -> given.copy(name = type.name)
        is NonNullType -> typeFromIDL(type.type, given.copy(nonNull = true))
        is ListType -> typeFromIDL(type.type, given.copy(array = true))
        else -> {
            println("Type ${type}"); given
        }
    }
}
