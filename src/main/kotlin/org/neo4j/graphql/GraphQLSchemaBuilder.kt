package org.neo4j.graphql

import graphql.Scalars.*
import graphql.execution.ExecutionTypeInfo
import graphql.execution.ValuesResolver
import graphql.introspection.Introspection
import graphql.language.*
import graphql.schema.*
import graphql.schema.GraphQLArgument.newArgument
import graphql.schema.GraphQLFieldDefinition.newFieldDefinition
import graphql.schema.GraphQLObjectType.newObject
import org.neo4j.graphdb.*
import org.neo4j.graphdb.Node
import org.neo4j.graphql.CypherGenerator.Companion.DEFAULT_CYPHER_VERSION
import org.neo4j.graphql.CypherGenerator.Companion.formatAnyValue
import org.neo4j.graphql.CypherGenerator.Companion.formatValue
import org.neo4j.helpers.collection.Iterators
import java.util.*

class GraphQLSchemaBuilder(val metaDatas: Collection<MetaData>) {

    object ArgumentProperties {
        interface ArgumentProperty {
            // clause: Clause
            val name : String
            val type : GraphQLType
            fun description() : String? = null
            fun render(variable: String) : Pair<String,String>?
            fun argument(variable: String, field:String, value:Any?) : String
            fun toArgument() = newArgument().name(name).type(type as GraphQLInputType).build()
            fun toField() = newFieldDefinition().name(name).description(description()).type(type as GraphQLOutputType).build()
            fun  matches(field: String): Boolean = name == field
        }
/*
        object Property : ArgumentProperty {
            override val name = null
            override val type = GraphQLLong
            override fun description() = "internal node id"

            override fun render(variable: String) = Pair(name, "id(`$variable`)")

            fun argument(variable: String, field:String? = null, value:Value) = "id(`$variable`) = ${formatValue(value)}"
        }
*/
        object NodeId : ArgumentProperty {
            override val name = "_id"
            override val type = GraphQLLong
            override fun description() = "internal node id"

            override fun render(variable: String) = Pair(name, "id(`$variable`)")

            override fun argument(variable: String, field:String, value:Any?) = "id(`$variable`) = ${formatAnyValue(value)}"
        }
        object NodeIds : ArgumentProperty {
            override val name = "_ids"
            override val type = GraphQLList(GraphQLLong)
            override fun description() = "internal node ids"

            override fun render(variable: String) = Pair(name, "[id(`$variable`)]")

            override fun argument(variable: String, field:String, value:Any?) = "id(`$variable`) IN ${formatAnyValue(value)}"
        }
    }

    fun graphqlTypeFor(arg: Type, existingTypes: Map<String, GraphQLType>): GraphQLType =
            when (arg) {
                is NonNullType -> GraphQLNonNull(graphqlTypeFor(arg.type, existingTypes))
                is ListType -> GraphQLList(graphqlTypeFor(arg.type, existingTypes))
                is TypeName ->
                    when (arg.name) { // todo other types
                        GraphQLInt.name -> GraphQLInt
                        GraphQLBoolean.name -> GraphQLBoolean
                        GraphQLString.name -> GraphQLString
                        GraphQLID.name -> GraphQLID
                    // todo others, also inline declared object types
                        else -> existingTypes.get(arg.name) ?: GraphQLTypeReference(arg.name)
                    // todo
                    }
                else -> throw RuntimeException("Unknown Type " + arg)
            }

    fun toDynamicQueryOrMutationFields(fields: List<FieldDefinition>, objectTypes: Map<String,GraphQLType>) : Map<String, GraphQLFieldDefinition> {

        fun firstColumn(result: Result) = result.columnAs<Any>(result.columns().first()).asSequence()

        fun asEntity(value:Any?) : Any? =
            when (value) {
                null -> null
                is Iterable<*> -> value.map(::asEntity)
                is Map<*, *> -> value.mapValues{ asEntity(it.value) }
                is Node -> value.allProperties + mapOf("_labels" to value.labels.map { it.name() }.toList(), "_id" to value.id)
                is Relationship -> value.allProperties + mapOf("_labels" to setOf(value.type.name()), "_id" to value.id)
                is Path -> mapOf("start" to asEntity(value.startNode()), "end" to asEntity(value.endNode()), "length" to value.length(),
                        "connections" to value.relationships().map(::asEntity),"nodes" to value.nodes().map(::asEntity).toList())
                else -> value
            }


        // turn result of update operation into the expected graphql type
        fun asEntityList(result: Result?, returnType: GraphQLOutputType): Any? {
            if (result == null || !result.hasNext())
                return if (returnType.isList()) emptyList<Map<String, Any>>() else null

            val isSingleColumn = result.columns().size == 1
            val innerType = returnType.inner()

            val list = if (isSingleColumn) {
                if (innerType is GraphQLFieldsContainer || innerType is GraphQLTypeReference) {
                    firstColumn(result).map(::asEntity)
                } else {
                    firstColumn(result)
                }
            } else result.asSequence().map { row -> row.mapValues{ asEntity(it.value) } }

            val res = if (returnType.isList()) list.toList() else list.firstOrNull()
            return res
        }

        fun executeCypher(fieldDefinition: FieldDefinition, env: DataFetchingEnvironment, returnType: GraphQLOutputType) : Any? {
            val db = env.getContext<GraphQLContext>().db
            val fieldName = fieldDefinition.name
            val field = env.fields.first { it.name == fieldName }

            fun execute(statement: String, params: Map<String,Any>, cb: ((Result) -> (Any?))? = null) : Any?{
                db.beginTx().use { tx ->
                    db.execute(statement, params).use { result ->
                        val cypherResult: Any? = cb?.invoke(result) ?: result.queryStatistics.toString()
                        tx.success()
                        return cypherResult
                    }
                }
            }

            val targetType = fieldDefinition.type.inner()
            val md = metaDatas.find { it.type == targetType }
            val cypher = fieldDefinition.cypher() ?: throw IllegalStateException("No @cypher annotation on field $fieldName")

            val needNesting = !cypher.passThrough && md?.let { env.selectionSet.get().values.any { selections -> selections.any { md.hasRelationship(it.name) } } } ?: false

            val arguments = fieldDefinition.inputValueDefinitions.associate { arg -> arg.name to env.getArgument<Any>(arg.name) }
            val params = arguments // + mapOf("__params__" to arguments)
            val isMutation = env.graphQLSchema?.mutationType == env.parentType
            val statement = if (needNesting) CypherGenerator.instance().generateQueryForField(field, fieldDefinition, isMutation, params = params) else cypher.statement
            return execute(statement, params, { result -> asEntityList(result, returnType)})
        }

        return fields.map { field ->
            val returnType = graphqlTypeFor(field.type, objectTypes) as GraphQLOutputType

            field.name to GraphQLFieldDefinition.newFieldDefinition()
                    .name(field.name)
                    .type(returnType)
                    .description(field.description())
                    .dataFetcher { ctx -> executeCypher(field, ctx, returnType) }
                    .argument(field.inputValueDefinitions.map { arg ->
                        // todo directives
                        GraphQLArgument.newArgument()
                                .name(arg.name)
                                .description(arg.description())
                                .type(graphqlTypeFor(arg.type,objectTypes) as GraphQLInputType)
                                .defaultValue(arg.defaultValue?.extract())
                                .build()
                    })
                    .build()
        }.toMap()
    }

    fun toGraphQLObjectType(metaData: MetaData, interfaceDefinitions: Map<String, GraphQLInterfaceType> = emptyMap()) : GraphQLObjectType {
        var builder: GraphQLObjectType.Builder = newObject()
                .name(metaData.type)
                .description(metaData.description ?: metaData.type + "-Node")

        builder = builder.field(ArgumentProperties.NodeId.toField())

        metaData.labels.mapNotNull { interfaceDefinitions.get(it) }.forEach {  builder = builder.withInterface(it) }

        // todo relationships, labels etc.

        // something is off with rule-checking probably interface names conflicting with object names
        //        builder = addInterfaces(builder);
        builder = addProperties(metaData, builder)
        builder = addRelationships(metaData, builder)
        return builder.build()
    }


    fun toGraphQLInterfaceType(metaData: MetaData, typeResolver: (String?) -> GraphQLObjectType?): GraphQLInterfaceType {
        val interfaceName = metaData.type
        var builder: GraphQLInterfaceType.Builder = GraphQLInterfaceType.newInterface()
                .name(interfaceName)
                .description(interfaceName + "-Node")

        builder = builder.field(ArgumentProperties.NodeId.toField())

        // todo relationships, labels etc.

        // something is off with rule-checking probably interface names conflicting with object names
        //        builder = addInterfaces(builder);
        builder = addProperties(metaData, builder)
        builder = addRelationships(metaData, builder)

        builder = builder.typeResolver { env ->
            val cypherResult = env.`object`
            val row = cypherResult as Map<String,Any>
            val allLabels = row?.get("_labels") as List<String>?
            // we also have to add the interfaces to the mutation on create
            val firstRemainingLabel: String? = allLabels?.filterNot { it == interfaceName }?.firstOrNull() // would be good to have a "Node" superlabel? like Relay has

            if(firstRemainingLabel.isNullOrEmpty()) {
                var builder: GraphQLObjectType.Builder = GraphQLObjectType.newObject()
                        .name(interfaceName)
                        .description(interfaceName + "-Node")

                builder = builder.field(ArgumentProperties.NodeId.toField())

                builder = addProperties(metaData, builder)
                builder = addRelationships(metaData, builder)
                builder.build()
            } else {
                typeResolver(firstRemainingLabel)
            }

        }
        return builder.build()
    }

    private fun addRelationships(md: MetaData, builder: GraphQLObjectType.Builder): GraphQLObjectType.Builder {
        var newBuilder = builder
        for ((key, info) in md.relationships) {
            newBuilder = newBuilder.field(newReferenceField(md, key, info.label, info.multi, info.parameters?.values, description = info.description, nonNull = info.nonNull))
        }
        return newBuilder
    }

    private fun addRelationships(md: MetaData, builder: GraphQLInterfaceType.Builder): GraphQLInterfaceType.Builder {
        var newBuilder = builder
        for ((key, info) in md.relationships) {
            newBuilder = newBuilder.field(newReferenceField(md, key, info.label, info.multi, info.parameters?.values, description = info.description, nonNull = info.nonNull))
        }
        return newBuilder
    }

    private fun addProperties(md: MetaData, builder: GraphQLObjectType.Builder): GraphQLObjectType.Builder {
        var newBuilder = builder
        for ((name, type) in md.properties) {
            newBuilder = newBuilder.field(newField(md, name, type))
        }
        return newBuilder
    }

    private fun addProperties(md: MetaData, builder: GraphQLInterfaceType.Builder): GraphQLInterfaceType.Builder {
        var newBuilder = builder
        for ((name, type) in md.properties) {
            newBuilder = newBuilder.field(newField(md, name, type))
        }
        return newBuilder
    }

    private fun newReferenceField(md: MetaData, name: String, label: String, multi: Boolean,
                                  parameters: Iterable<MetaData.ParameterInfo>? = emptyList(),
                                  description: String? = null, nonNull: Int = 0

    ): GraphQLFieldDefinition {
        val labelMd = metaDatas.find { it.type == label }!!
        val innerType = GraphQLTypeReference(label)
        val innerType1: GraphQLOutputType = if (nonNull>1) GraphQLNonNull(innerType) else innerType
        val listType: GraphQLOutputType = if (multi) GraphQLList(innerType1) else innerType1
        val type: GraphQLOutputType = if (nonNull>0) GraphQLNonNull(listType) else listType
        val hasProperties = labelMd.properties.isNotEmpty()
        val field = newFieldDefinition()
                .name(name)
                /*
            .dataFetcher((env) -> {
                return ((List<Map<String,Object>>)env.getSource()).stream()
                        .flatMap( (row) ->
                                env.getFields().stream().map(f -> row.get(f.getName())))
                        .collect(Collectors.toList());
            })
*/
                .description(description ?: md.type + " " + name + " " + label)
                .argument(propertiesAsArguments(labelMd))
                .argument(propertiesAsListArguments(labelMd))
                .argumentIf(hasProperties, {orderByArgument(labelMd)})
                .argumentIf(hasProperties, {filterArgument(labelMd)})
                .argument(toArguments(parameters))
                .type(type)

        return if (multi) {
            withFirstOffset(field).build()
        } else field.build()
    }

    fun GraphQLFieldDefinition.Builder.argumentIf(pred: Boolean, arg: () -> GraphQLArgument) =
        if (pred) this.argument(arg.invoke()) else this

    private fun toArguments(parameters: Iterable<MetaData.ParameterInfo>?): List<GraphQLArgument> {
        return parameters?.map { newArgument().name(it.name).type(graphQlInType(it.type, false)).defaultValue(it.defaultValue).build() } ?: emptyList()
    }

    private fun withFirstOffset(field: GraphQLFieldDefinition.Builder) = field
            .argument(ArgumentProperties.NodeId.toArgument())
            .argument(newArgument().name("_ids").type(GraphQLList(GraphQLLong)).build())
            .argument(newArgument().name("first").type(GraphQLInt).build())
            .argument(newArgument().name("offset").type(GraphQLInt).build())

    private fun newField(md: MetaData, name: String, prop: MetaData.PropertyInfo): GraphQLFieldDefinition {
        return newFieldDefinition()
                .name(name)
                /*
            .dataFetcher((env) -> {
                return ((List<Map<String,Object>>)env.getSource()).stream()
                        .flatMap( (row) ->
                                env.getFields().stream().map(f -> row.get(f.getName())))
                        .collect(Collectors.toList());
            })
*/
                .description(prop.description ?: name + " of  " + md.type)
                //                      .type(ids.contains(name) ? Scalars.GraphQLID : graphQlType(value.getClass()))
                //                      .fetchField().dataFetcher((env) -> null)
                .type(graphQlOutType(prop.type))
                .argument(toArguments(prop.parameters?.values))
                .build()
    }

    companion object {
        class GraphQLSchemaWithDirectives(queryType: GraphQLObjectType, mutationType: GraphQLObjectType, additionalTypes: Set<GraphQLType>, newDirectives : List<GraphQLDirective>)
            : GraphQLSchema(queryType, mutationType, additionalTypes) {

            val myDirectives : List<GraphQLDirective>
            init {
                this.myDirectives = newDirectives + super.getDirectives()
            }
            override fun getDirectives(): List<GraphQLDirective> {
                return myDirectives
            }
        }

        @JvmStatic fun buildSchema(db: GraphDatabaseService): GraphQLSchema {
            GraphSchemaScanner.databaseSchema(db)

            return GraphQLSchemaBuilder(GraphSchemaScanner.allMetaDatas()).buildSchema()
        }

        fun resolveParameters(schema: GraphQLSchema, fields: List<Field>, variables: Map<String, Any>, fieldTypeInfo: ExecutionTypeInfo) : Map<String,Any> {
            val valuesResolver = ValuesResolver()
            val result = mutableMapOf<String,Any>()
            fun resolve(field : Field) : Unit {
                val parentType = fieldTypeInfo.castType(GraphQLObjectType::class.java)
                val fieldDef = schema.getFieldVisibility().getFieldDefinition(parentType, field.getName());

                result.putAll(valuesResolver.getArgumentValues(fieldDef.getArguments(), field.getArguments(), variables))
                field.selectionSet.selections.filterIsInstance<Field>().forEach { resolve(it) }
            }
            fields.forEach { resolve(it) }
            return result
        }

        private fun graphQLDirectives() = listOf<GraphQLDirective>(
                newFieldDirective("relation", "Relationship"),
                newFieldDirective("defaultValue", "default value"),
                newFieldDirective("isUnique", "field is unique in type"),
                newFieldDirective("model", "entity is a model type"),
                newFieldDirective("cypher", "Cypher query to run"),
                newDirective("profile", "Enable query profiling"),
                newDirective("explain", "Enable query explanation"),
                newDirective("version", "Specify Cypher version", GraphQLArgument("version","Cypher Version (3.0, 3.1, 3.2)", GraphQLString, DEFAULT_CYPHER_VERSION))
        )

        private fun newDirective(name: String, desc: String, vararg arguments: GraphQLArgument) =
                GraphQLDirective(name, desc, EnumSet.of(Introspection.DirectiveLocation.QUERY), listOf(*arguments), true, false, true)

        private fun newFieldDirective(name: String, desc: String, vararg arguments: GraphQLArgument) =
                GraphQLDirective(name, desc, EnumSet.of(Introspection.DirectiveLocation.FIELD), listOf(*arguments), true, false, true)
    }

    fun obtainEnum(given: GraphQLEnumType) : GraphQLEnumType {
        return enums.computeIfAbsent(given.name, {given})
    }

    fun inputType(name: String) : GraphQLInputType {
        return (enums.get(name) ?: inputTypes.get(name) ?: GraphQLTypeReference(name)) as GraphQLInputType
    }

    val typeMetaDatas = metaDatas.filterNot {  it.isInterface }
    val definitions = IDLParser.parseDefintions(GraphSchemaScanner.schema)
    val enums: MutableMap<String, GraphQLEnumType> = enumsFromDefinitions(definitions).toMutableMap()
    val scalars = scalarsFromDefinitions(definitions)
    val inputTypes: MutableMap<String, GraphQLInputObjectType> = inputTypesFromDefinitions(definitions, enums).toMutableMap()

    fun buildSchema() : GraphQLSchema {

        val dictionary = graphQlTypes(metaDatas)

        val interfaceTypes = dictionary.first
        val objectTypes = dictionary.second

        val mutationsFromSchema = GraphSchemaScanner.schema?.
                let { IDLParser.parseMutations(it) }?.
                let { parsedMutations -> toDynamicQueryOrMutationFields(parsedMutations, objectTypes) } ?: emptyMap()

        val queriesFromSchema = GraphSchemaScanner.schema?.
                let { IDLParser.parseQueries(it) }?.
                let { parsedQueries -> toDynamicQueryOrMutationFields(parsedQueries, objectTypes) } ?: emptyMap()

        var existingMutations = mutationsFromSchema.keys
        val generatedTypeMutations = GraphSchemaScanner.allMetaDatas().flatMap { mutationField(it,existingMutations) }
        existingMutations += generatedTypeMutations.map { it.name }
        val generatedRelationshipMutations = GraphSchemaScanner.allMetaDatas().flatMap { relationshipMutationFields(it, enums, existingMutations) }

        val mutationType: GraphQLObjectType = newObject().name("MutationType")
                .fields(generatedTypeMutations + generatedRelationshipMutations + mutationsFromSchema.values)
                .build()

        val queriesFromTypes = queryFields(metaDatas, queriesFromSchema)

        val queryType = newObject().name("QueryType")
                .fields(queriesFromTypes + queriesFromSchema.values)
                .build()

        // todo this was missing, it was only called by the builder: SchemaUtil().replaceTypeReferences(graphQLSchema)

        val allTypes = objectTypes + interfaceTypes + enums + inputTypes

        val schema = GraphQLSchema.Builder().mutation(mutationType).query(queryType).build(allTypes.values.toSet()) // interfaces seem to be quite tricky

        return GraphQLSchemaWithDirectives(schema.queryType, schema.mutationType, schema.additionalTypes, graphQLDirectives())
    }

    fun enumsFromDefinitions(definitions: List<Definition>) = IDLParser.filterEnums(definitions).associate { e ->
        e.name to GraphQLEnumType(e.name, e.description() ?: "Enum for ${e.name}",
                e.enumValueDefinitions.map { ev -> GraphQLEnumValueDefinition(ev.name, ev.description() ?: "Value for ${ev.name}", ev.name) })
    }

    fun scalarsFromDefinitions(definitions: List<Definition>) = IDLParser.filterScalars(definitions).associate {
        it.name to GraphQLScalarType(it.name, it.description() ?: "Scalar ${it.name}", NoOpCoercing, it)}

    fun inputTypesFromDefinitions(definitions: List<Definition>, inputTypes: Map<String, GraphQLType> = emptyMap()) =
            IDLParser.filterInputTypes(definitions).associate { input ->
                input.name to GraphQLInputObjectType.newInputObject().name(input.name).description(input.description() ?: "Input Type " + input.name).fields(
                        input.inputValueDefinitions.map { field ->
                            GraphQLInputObjectField.newInputObjectField()
                                    .name(field.name).description(field.description() ?: "Field ${field.name} of ${input.name}")
                                    .type(graphqlTypeFor(field.type, inputTypes) as GraphQLInputType).defaultValue(field.defaultValue?.extract()).build()
                        })
                        .build()
            }

    private fun graphQlOutType(type: MetaData.PropertyType): GraphQLOutputType {
        var outType : GraphQLOutputType = if (type.enum) GraphQLTypeReference(type.name) else graphQLType(type)
        if (type.nonNull>1)
            outType = GraphQLNonNull(outType)
        if (type.array) {
            outType = GraphQLList(outType)
        }
        if (type.nonNull>0)
            outType = GraphQLNonNull(outType)
        return outType
    }

    private fun graphQLType(type: MetaData.PropertyType): GraphQLScalarType {
        return scalars.get(type.name) ?:
         when (type.name) {
            "String" -> GraphQLString
            "ID" -> GraphQLID
            "Boolean" -> GraphQLBoolean
            "Number" -> GraphQLFloat
            "Float" -> GraphQLFloat
            "Long" -> GraphQLLong
            "Int" -> GraphQLInt
            else -> throw IllegalArgumentException("Unknown field type " + type)
        }
    }

    private fun graphQlInType(type: MetaData.PropertyType, required: Boolean = true): GraphQLInputType {
        var inType : GraphQLInputType = if (type.enum || type.inputType) GraphQLTypeReference(type.name) else graphQLType(type) // todo handle enums differently
        if (type.nonNull>1 && required)
            inType = GraphQLNonNull(inType)
        if (type.array) {
            inType = GraphQLList(inType)
        }
        if (type.nonNull>0 && required)
            inType = GraphQLNonNull(inType)
        return inType
    }

    fun queryFields(metaDatas: Iterable<MetaData>, queriesFromSchema: Map<String, GraphQLFieldDefinition> = emptyMap()): List<GraphQLFieldDefinition> {
        val existing = queriesFromSchema.keys
        return metaDatas
                .map { md ->
                    val hasProperties = md.properties.isNotEmpty()
                    withFirstOffset(
                            newFieldDefinition()
                            .name(handleCollisions(existing,md.type))
                            .type(GraphQLList(GraphQLTypeReference(md.type))) // todo
                            .argument(propertiesAsArguments(md))
                            .argument(propertiesAsListArguments(md))
                            .argumentIf(hasProperties,{orderByArgument(md)})
                            .argumentIf(hasProperties, {filterArgument(md)})
                            .dataFetcher({ env -> fetchGraphData(md, env) })
                    ).build()
                }
    }

    fun argumentValue(env:DataFetchingEnvironment, name: String) =
            env.getArgument<Any>(name).let { v -> if (v is Value) v.extract() else v }
    fun toArguments(props: Iterable<MetaData.PropertyInfo>, env: DataFetchingEnvironment) = props.associate {
        it.fieldName to argumentValue(env, it.fieldName) }

    fun mutationField(metaData: MetaData, existing: Set<String>) : List<GraphQLFieldDefinition> {
        val idProperty = metaData.idProperty()

        val updatableProperties = metaData.properties.values.filter { !it.isComputed() }

        val createMutation = GraphQLFieldDefinition.newFieldDefinition()
                .name(handleCollisions(existing,"create" + metaData.type))
                .description("Creates a ${metaData.type} entity")
                .type(GraphQLString)
                .argument(updatableProperties.map { GraphQLArgument(it.fieldName, graphQlInType(it.type)) })
                .dataFetcher{ env ->
                    val statement = "CREATE (node:${metaData.type}) SET node = {properties} " + metaData.labels.map { "SET node:`$it`" }.joinToString(", ")
                    val params = mapOf<String, Any>("properties" to toArguments(updatableProperties,env))
                    executeUpdate(env, statement, params)
                }
                .build()


        if (idProperty == null)
            return listOf(createMutation)
        else {
            val nonIdProperties = updatableProperties.filterNot { it == idProperty }

            val updateMutation = GraphQLFieldDefinition.newFieldDefinition()
                    .name(handleCollisions(existing,"update" + metaData.type))
                    .description("Updates a ${metaData.type} entity")
                    .type(GraphQLString)
                    .argument(GraphQLArgument(idProperty.fieldName, graphQlInType(idProperty.type)))
                    .argument(nonIdProperties.map { GraphQLArgument(it.fieldName, graphQlInType(it.type)) })
                    .dataFetcher { env ->
                        val params = mapOf<String, Any>(
                                "id" to argumentValue(env,idProperty.fieldName),
                                "properties" to toArguments(nonIdProperties,env))

                        val statement = "MATCH (node:`${metaData.type}` {`${idProperty.fieldName}`:{id}}) SET node += {properties}"

                        executeUpdate(env, statement, params)
                    }
                    .build()
            val deleteMutation = GraphQLFieldDefinition.newFieldDefinition()
                    .name(handleCollisions(existing,"delete" + metaData.type))
                    .description("Deletes a ${metaData.type} entity")
                    .type(GraphQLString)
                    .argument(GraphQLArgument(idProperty.fieldName, graphQlInType(idProperty.type)))
                    .dataFetcher { env ->
                        val params = mapOf<String, Any>("id" to argumentValue(env,idProperty.fieldName))

                        val statement = "MATCH (node:`${metaData.type}` {`${idProperty.fieldName}`:{id}}) DETACH DELETE node"

                        executeUpdate(env, statement, params)

                    }
                    .build()

            return listOf(createMutation, updateMutation, deleteMutation)
        }

    }

    private fun handleCollisions(existing: Set<String>, name: String) = if (existing.contains(name)) name + "_" else name

    fun relationshipMutationFields(metaData: MetaData, inputs: Map<String, GraphQLInputType>, existing: Set<String>) : List<GraphQLFieldDefinition> {
        val idProperty = metaData.idProperty()
        return  metaData.relationships.values.flatMap {  rel ->
            val targetMeta = GraphSchemaScanner.getMetaData(rel.label)!!
            val targetIdProperty = targetMeta.idProperty()
            if (idProperty == null || targetIdProperty == null) emptyList()
            else {
                val sourceArgument = GraphQLArgument(idProperty.fieldName, graphQlInType(idProperty.type))
                val targetArguments = GraphQLArgument(rel.fieldName, GraphQLNonNull(GraphQLList(graphQlInType(targetIdProperty.type))))
                val (left,right) = if (rel.out) Pair("",">") else Pair("<","")
                listOf(
                GraphQLFieldDefinition.newFieldDefinition()
                        .name(handleCollisions(existing, "add" + metaData.type + rel.fieldName.capitalize()))
                        .description("Adds ${rel.fieldName.capitalize()} to ${metaData.type} entity")
                        .type(GraphQLString)
                        .argument(sourceArgument).argument(targetArguments)
                        .dataFetcher { env ->
                            val statement = """MATCH (from:`${metaData.type}` {`${idProperty.fieldName}`:{source}})
                                               MATCH (to:`${targetMeta.type}`) WHERE to.`${targetIdProperty.fieldName}` IN {targets}
                                               MERGE (from)$left-[:`${rel.type}`]-$right(to)"""

                            val params = mapOf<String,Any>("source" to argumentValue(env,idProperty.fieldName), "targets" to  argumentValue(env,rel.fieldName))
                            executeUpdate(env, statement, params)
                        }
                        .build(),
                GraphQLFieldDefinition.newFieldDefinition()
                        .name(handleCollisions(existing, "delete" + metaData.type + rel.fieldName.capitalize()))
                        .description("Deletes ${rel.fieldName.capitalize()} from ${metaData.type} entity")
                        .type(GraphQLString)
                        .argument(sourceArgument).argument(targetArguments)
                        .dataFetcher { env ->
                            val statement = """MATCH (from:`${metaData.type}` {`${idProperty.fieldName}`:{source}})
                                               $left-[rel:`${rel.type}`]-$right(to:`${targetMeta.type}`)
                                               WHERE to.`${targetIdProperty.fieldName}` IN {targets}
                                               DELETE rel
                                            """

                            val params = mapOf<String,Any>("source" to argumentValue(env,idProperty.fieldName), "targets" to  argumentValue(env,rel.fieldName))
                            executeUpdate(env, statement, params)
                        }
                        .build()
                )
            }
        }
    }

    private fun executeUpdate(env: DataFetchingEnvironment, statement: String, params: Map<String, Any>): String {
        val db = env.getContext<GraphQLContext>().db

        db.execute(statement, params).use {
            return it.queryStatistics.toString()
        }
    }


    private fun fetchGraphData(md: MetaData, env: DataFetchingEnvironment): List<Map<String, Any>> {
        val ctx = env.getContext<GraphQLContext>()
        val db = ctx.db
        val fragments = env.fragmentsByName
        val generator = CypherGenerator.instance()
        val parameters = ctx.parameters.toMutableMap()
        parameters.putAll(env.arguments)
        return env.fields
                .map { it to generator.generateQueryForField(it, env.fieldDefinition.definition, fragments = env.fragmentsByName, params = parameters) }
                .flatMap({ pair ->
                    val (field, query) = pair
                    val directives = field.directives.associate { it.name to it }
                    val statement = applyDirectivesToStatement(generator, query, directives)
                    ctx.log?.debug(statement)
//                    println(statement)
//                    val parameters = resolveParameters(env.graphQLSchema, env.fields,ctx.parameters, env.fieldTypeInfo)
                    val result = db.execute(statement, parameters)
                    val list = Iterators.asList(result)
                    storeResultMetaData(ctx, query, result, directives)
                    list
                })
    }

    private fun applyDirectivesToStatement(generator: CypherGenerator, query: String, directives: Map<String, Directive>) :String {
        val parts = mutableListOf<String>()
//        if (directives.containsKey("cypher"))  { parts.add(directives.get("cypher").arguments.first().value.toString())  }
        if (directives.containsKey("explain")) { parts.add("explain")  }
        if (directives.containsKey("profile")) { parts.remove("explain"); parts.add("profile")  }

        return if (parts.isEmpty()) query else parts.joinToString(" ","cypher "," ") + query
    }

    // todo make it dependenden on directive
    private fun storeResultMetaData(ctx: GraphQLContext, query: String, result: Result, directives: Map<String, Directive>) {
        ctx.store("type", result.queryExecutionType.queryType().name) // todo other query type information
        if (directives.containsKey("explain") || directives.containsKey("profile")) {
            ctx.store("columns", result.columns())
            ctx.store("query", query)
            ctx.store("warnings", result.notifications.map { "${it.severity.name}-${it.code}(${it.position.line}:${it.position.column}) ${it.title}:\n${it.description}" })
            ctx.store("plan", result.executionPlanDescription.toString())
        }
        if (result.queryStatistics.containsUpdates()) {
            ctx.store("stats", result.queryStatistics.toString()) // todo other query type information
        }
    }

    fun graphQlTypes(metaDatas: Iterable<MetaData>): Pair<Map<String,GraphQLInterfaceType>,Map<String,GraphQLObjectType>> {
        val interfaces = metaDatas.filter { it.isInterface }
        val nonInterfaces = metaDatas.filter { !it.isInterface }

        val mutableObjectTypes = mutableMapOf<String,GraphQLObjectType>()
        val interfaceDefinitions = interfaces.associate { it.type to toGraphQLInterfaceType(it, { mutableObjectTypes.get(it) }) }

        val objectTypes = nonInterfaces.associate { it.type  to toGraphQLObjectType(it, interfaceDefinitions) }
        mutableObjectTypes.putAll(objectTypes) // kinda weird though the cyclcic dependency, we should add _labels  to the cypher result and then decide it from there, on
        return Pair(interfaceDefinitions, objectTypes)
    }

    internal fun propertiesAsArguments(md: MetaData): List<GraphQLArgument> {
        return md.properties.values.map {
            newArgument().name(it.fieldName).description(it.fieldName + " of " + md.type).type(graphQlInType(it.type, false)).build()
        }
    }
    internal fun orderByArgument(md: MetaData): GraphQLArgument {
        return newArgument().name("orderBy")
                .type(GraphQLList(obtainEnum(GraphQLEnumType("_${md.type}Ordering","Ordering Enum for ${md.type}",
                        md.properties.keys.flatMap { listOf(
                                GraphQLEnumValueDefinition(it+"_asc","Ascending sort for $it",Pair(it,true)),
                                GraphQLEnumValueDefinition(it+"_desc","Descending sort for $it",Pair(it,false))) })))).build()
    }

    internal fun inputField(name:String, type:GraphQLInputType, desc: String = name) : GraphQLInputObjectField
            = GraphQLInputObjectField.newInputObjectField().name(name).type(type).description(desc).build()

    internal fun filterArgument(md: MetaData): GraphQLArgument {
        return newArgument().name("filter").type(filterInputObjectType(md)).build()
    }

    private fun filterInputObjectType(md: MetaData): GraphQLInputType {
        return inputTypes.computeIfAbsent(filterName(md.type), {
            inputTypeName ->
            GraphQLInputObjectType(inputTypeName, "Filter Input Type for ${md.type}",
                    listOf(inputField("AND", GraphQLList(GraphQLNonNull(GraphQLTypeReference(inputTypeName)))),
                            inputField("OR", GraphQLList(GraphQLNonNull(GraphQLTypeReference(inputTypeName)))))
                            + md.properties.values.flatMap { p ->
                                val fieldType: GraphQLInputType =
                                if (p.type.isBasic()) graphQLType(p.type)
                                else if (p.type.enum) obtainEnum(GraphQLEnumType.newEnum().name(p.type.name).build())
                                else GraphQLTypeReference(p.type.name)
                                Operators.forType(fieldType).map { op -> inputField(op.fieldName(p.fieldName), if (op.list) GraphQLList(GraphQLNonNull(fieldType)) else fieldType) }
                            }
                            + md.relationships.values.flatMap { ri ->
                                val fieldType: GraphQLInputType = GraphQLTypeReference(filterName(ri.label))
                                val ops = Operators.forType(fieldType) + if (ri.multi) listOf(Operators.SOME, Operators.NONE, Operators.SINGLE, Operators.ALL) else emptyList()
                                ops.map { op -> inputField(op.fieldName(ri.fieldName), fieldType) }
                            }

                    )
        })
    }

    private fun filterName(name: String) = "_${name}Filter"

    internal fun propertiesAsListArguments(md: MetaData): List<GraphQLArgument> {
        return md.properties.values.map {
            newArgument().name(it.fieldName+"s").description(it.fieldName + "s is list variant of "+it.fieldName + " of " + md.type).type(GraphQLList(graphQlInType(it.type, false))).build()
        }
    }
}
object NoOpCoercing : Coercing<Any,Any> {
    override fun parseLiteral(input: Any?) = input

    override fun serialize(dataFetcherResult: Any?) = dataFetcherResult

    override fun parseValue(input: Any?) = input
}

