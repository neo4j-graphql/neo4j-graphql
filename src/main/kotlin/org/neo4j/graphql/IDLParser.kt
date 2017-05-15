package org.neo4j.graphql

import graphql.Scalars.*
import graphql.language.*
import graphql.parser.Parser
import graphql.schema.*
import org.neo4j.graphql.MetaData.*

object IDLParser {
    fun parseMutations(input : String ) : List<FieldDefinition> {
        val definitions = Parser().parseDocument(input).definitions
        val mutationName: String? =
                definitions.filterIsInstance<SchemaDefinition>()
                        .flatMap { it.operationTypeDefinitions.filter { it.name == "mutation" }
                                .map { (it.type as TypeName).name } }.firstOrNull()
        return definitions.filterIsInstance<ObjectTypeDefinition>().filter { it.name == mutationName  }.flatMap { it.fieldDefinitions  }
    }

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
            val labels = definition.implements.filterIsInstance<TypeName>().map { it.name }
            metaData.labels.addAll(labels)
        }

        if (definition is InterfaceTypeDefinition) {
            metaData.isInterface()
        }

        definition.children.forEach { child ->
            when (child) {
                is FieldDefinition -> {
                    val fieldName = child.name
                    val type = typeFromIDL(child.type)

                    if (type.isBasic()) {
                        metaData.addProperty(fieldName, type)
                    } else {
                        val relation = directivesByName(child, "relation").firstOrNull()

                        if (relation == null) {
                            metaData.mergeRelationship(fieldName, fieldName, type.name, out = true, multi = type.array)
                        } else {

                            val typeName = argumentByName(relation, "name").map { it.value.extract() as String }.firstOrNull() ?: fieldName
                            val out = argumentByName(relation, "direction").map { !((it.value.extract() as String).equals( "IN", ignoreCase = true)) }.firstOrNull() ?: true

                            metaData.mergeRelationship(typeName, fieldName, type.name, out, type.array)
                        }
                    }
                    if (type.nonNull) {
                        metaData.addIdProperty(fieldName)
                    }
                    directivesByName(child, "cypher")
                            .map { cypher -> argumentByName(cypher,"statement").map{ it.value.extract() as String}.first() }
                            .forEach { metaData.addCypher(fieldName, it)}

                    metaData.addParameters(fieldName,
                            child.inputValueDefinitions.associate {
                                it.name to ParameterInfo(it.name, typeFromIDL(it.type), it.defaultValue?.extract()) })
                }
                is TypeName -> println("TypeName: " + child.name + " " + child.javaClass + " in " + definition.name)
                is EnumValueDefinition -> println("EnumValueDef: " + child.name + " " + child.directives.map { it.name })
                else -> println(child.javaClass.name)
            }
        }
        return metaData
    }

    private fun argumentByName(relation: Directive, argumentName: String) = relation.arguments.filter { it.name == argumentName }

    private fun directivesByName(child: FieldDefinition, directiveName: String) = child.directives.filter { it.name == directiveName }

    private fun typeFromIDL(type: Type, given: MetaData.PropertyType = PropertyType("String")): MetaData.PropertyType = when (type) {
        is TypeName -> given.copy(name = type.name)
        is NonNullType -> typeFromIDL(type.type, given.copy(nonNull = true))
        is ListType -> typeFromIDL(type.type, given.copy(array = true))
        else -> {
            println("Type ${type}"); given
        }
    }
}
