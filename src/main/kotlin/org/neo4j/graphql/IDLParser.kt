package org.neo4j.graphql

import graphql.language.*
import graphql.parser.Parser
import graphql.parser.antlr.GraphqlParser
import org.neo4j.graphql.MetaData.*

object IDLParser {
    fun parseMutations(input : String ) : List<FieldDefinition> {
        return parseSchemaType(input, "mutation")
    }

    fun filterEnums(definitions: List<Definition>)
            = definitions
            .filterIsInstance<EnumTypeDefinition>()

    fun filterScalars(definitions: List<Definition>)
            = definitions
            .filterIsInstance<ScalarTypeDefinition>()

    // todo directives
    fun parseEnums(definitions: List<Definition>)
            = definitions
            .filterIsInstance<EnumTypeDefinition>()
            .associate { it.name to it.enumValueDefinitions.map { it.name } }

    fun filterInputTypes(definitions: List<Definition>)
            = definitions
            .filterIsInstance<InputObjectTypeDefinition>()

    fun parseInputTypes(definitions: List<Definition>)
            = definitions
            .filterIsInstance<InputObjectTypeDefinition>()
            .associate { it.name to it.inputValueDefinitions}

    fun parseQueries(input : String ) : List<FieldDefinition> {
        return parseSchemaType(input, "query")
    }

    fun parseSchemaType(input: String, schemaType: String): List<FieldDefinition> {
        val definitions = parseDocument(input).definitions
        val mutationName: String? =
                definitions.filterIsInstance<SchemaDefinition>()
                        .flatMap {
                            it.operationTypeDefinitions.filter { it.name == schemaType }
                                    .map { (it.type as TypeName).name }
                        }.firstOrNull()
        return definitions.filterIsInstance<ObjectTypeDefinition>().filter { it.name == mutationName }.flatMap { it.fieldDefinitions }
    }

    private fun parseSchemaTypes(definitions: MutableList<Definition>) =
            definitions.filterIsInstance<SchemaDefinition>()
            .map {
                it.operationTypeDefinitions.associate { it.name to (it.type as TypeName).name }
            }.firstOrNull() ?: emptyMap()

    fun parse(input: String): Map<String,MetaData> {
        val definitions = parseDocument(input).definitions
        val schemaTypes = parseSchemaTypes(definitions)
        val enums = filterEnums(definitions).map { it.name }.toSet()
        val scalars = filterScalars(definitions).map { it.name }.toSet()
        return (definitions.map {
            when (it) {
//            is TypeDefinition -> toMeta(x)
//            is UnionTypeDefinition -> toMeta(x)
//            is EnumTypeDefinition -> toMeta(x)
                is InterfaceTypeDefinition -> toMeta(it,enums, scalars)
//            is InputObjectTypeDefinition -> toMeta(x)
//            is ScalarTypeDefinition -> toMeta(x)
                is ObjectTypeDefinition -> if (schemaTypes.values.contains(it.name)) null else toMeta(it,enums,scalars)
                else -> {
                    println(it.javaClass); null
                }
            }
        }).filterNotNull().associate { m -> Pair(m.type, m) }
    }

    private fun parseDocument(input: String) : Document {
        val parser = Parser()
        try {
            return parser.parseDocument(input)
        } catch (e:Exception) {
            val cause = e.cause
            when (cause) {
                is org.antlr.v4.runtime.RecognitionException -> {
                    val token = cause.offendingToken
                    val expected = cause.expectedTokens.toString(GraphqlParser.VOCABULARY)
                    throw RuntimeException(String.format("Error parsing IDL expected %s got '%s' line %d column %d",  expected, token.text, token.line,token.charPositionInLine),e);
                }
                else -> throw e;
            }
        }
    }

    fun toMeta(definition: TypeDefinition, enumNames: Set<String> = emptySet(), scalarNames : Set<String> = emptySet()): MetaData {
        val metaData = MetaData(definition.name)
        metaData.description = definition.description()
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
                    val description = child.description()
                    val type = typeFromIDL(child.type, enumNames, scalarNames)
                    val defaultValue = directivesByName(child,"defaultValue").flatMap{ it.arguments.map { it.value.extract()  }}.firstOrNull()
                    val isUnique = directivesByName(child,"isUnique").isNotEmpty()
                    if (type.isBasic() || type.enum) {
                        metaData.addProperty(fieldName, type, defaultValue, unique = isUnique, enum = type.enum, description = description)
                    } else {
                        val relation = directivesByName(child, "relation").firstOrNull()

                        if (relation == null) {
                            metaData.mergeRelationship(fieldName, fieldName, type.name, out = true, multi = type.array, description = description, nonNull =  type.nonNull)
                        } else {

                            val typeName = argumentByName(relation, "name").map { it.value.extract() as String }.firstOrNull() ?: fieldName
                            val out = argumentByName(relation, "direction").map { !((it.value.extract() as String).equals( "IN", ignoreCase = true)) }.firstOrNull() ?: true

                            metaData.mergeRelationship(typeName, fieldName, type.name, out, type.array, description = description,nonNull =  type.nonNull)
                        }
                    }
                    if (type.nonNull > 0 && type.isBasic()) {
                        metaData.addIdProperty(fieldName)
                    }
                    directivesByName(child, "cypher")
                            .map { cypher -> argumentByName(cypher,"statement").map{ it.value.extract() as String}.first() }
                            .forEach { metaData.addCypher(fieldName, it)}

                    metaData.addParameters(fieldName,
                            child.inputValueDefinitions.associate {
                                it.name to ParameterInfo(it.name, typeFromIDL(it.type, enumNames,scalarNames), it.defaultValue?.extract(), it.description()) })
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

    private fun typeFromIDL(type: Type, enums: Set<String>, scalars: Set<String>, given: MetaData.PropertyType = PropertyType("String")): MetaData.PropertyType = when (type) {
        is TypeName -> given.copy(name = type.name, enum = enums.contains(type.name), scalar = scalars.contains(type.name))
        is NonNullType -> typeFromIDL(type.type, enums,scalars, given.copy(nonNull = given.nonNull + 1))
        is ListType -> typeFromIDL(type.type, enums,scalars, given.copy(array = true))
        else -> {
            println("Type ${type}"); given
        }
    }

    fun parseDefintions(schema: String?) = if (schema==null) emptyList<Definition>() else Parser().parseDocument(schema).definitions
}
