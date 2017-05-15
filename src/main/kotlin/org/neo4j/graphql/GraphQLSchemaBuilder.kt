package org.neo4j.graphql

import graphql.Scalars
import graphql.Scalars.*
import graphql.introspection.Introspection
import graphql.language.*
import graphql.schema.*
import graphql.schema.GraphQLArgument.newArgument
import graphql.schema.GraphQLFieldDefinition.newFieldDefinition
import graphql.schema.GraphQLObjectType.newObject
import org.neo4j.graphdb.*
import org.neo4j.graphql.CypherGenerator.Companion.DEFAULT_CYPHER_VERSION
import org.neo4j.helpers.collection.Iterators
import java.util.*

class GraphQLSchemaBuilder {

//    fun toGraphQLInterface(md: MetaData): GraphQLInterfaceType {
//        var builder: GraphQLInterfaceType.Builder = GraphQLInterfaceType.newInterface()
//                .typeResolver { toGraphQL(GraphSchemaScanner.getMetaData(it.toString())!!) }
//                .name(md.type)
//                .description(md.type + "-Label")
//        // it uses the interface types for checking against parent-types of fields, which is weird
//        builder = addInterfaceProperties(md, builder)
//        return builder.build()
//    }

    private fun addInterfaceProperties(md: MetaData, builder: GraphQLInterfaceType.Builder): GraphQLInterfaceType.Builder {
        // mostly id, indexed, not sure about others
        // ids: Set<String>, indexed: Set<String>
        var newBuilder = builder
        for (prop in md.properties.values.filter { it.indexed }) {
            newBuilder = newBuilder.field(newField(md, prop.fieldName, prop))
        }
        return newBuilder
    }

    fun toMutationFields(mutations: List<FieldDefinition>, objectTypes: Map<String,GraphQLObjectType>) : List<GraphQLFieldDefinition> {
        fun graphqlTypeFor(arg: Type) : GraphQLType =
            when (arg) {
                is NonNullType -> GraphQLNonNull(graphqlTypeFor(arg.type))
                is ListType -> GraphQLList(graphqlTypeFor(arg.type))
                is TypeName ->
                    when (arg.name) { // todo other types
                        GraphQLInt.name -> GraphQLInt
                        GraphQLBoolean.name -> GraphQLBoolean
                        GraphQLString.name -> GraphQLString
                        GraphQLID.name -> GraphQLID
                        // todo others, also inline declared object types
                        else -> objectTypes.get(arg.name) ?: GraphQLString
                    // todo
                    }
                else -> throw RuntimeException("Unknown Type "+arg)
            }

        // turn result of update operation into the expected graphql type
        fun asEntityList(db: GraphDatabaseService, result: Result?, returnType: GraphQLOutputType): Any? {
            if (result ==null || !result.hasNext()) return emptyList<Map<String,Any>>()
            val cols = result.columns()
            val tx = db.beginTx()
            try {
                val (isList,type) = if (returnType is GraphQLList) true to returnType.wrappedType else false to returnType

                val list = if (cols.size == 1) {
                    result.columnAs<Any>(cols.get(0)).map {
                        if (type is GraphQLObjectType) {
                            when (it) {
                                is Map<*, *> -> it as Map<String, Any>
                                is Entity -> it.allProperties
                                else -> mapOf(cols.get(0) to it)
                            }
                        } else {
                            it
                        }
                    }.asSequence().toList()
                } else Iterators.asList(result)
                return if (isList) list else list.firstOrNull()
            } finally {
                tx.success()
                tx.close()
            }
        }

        fun databaseUpdate(field: FieldDefinition, ctx: DataFetchingEnvironment, returnType: GraphQLOutputType) : Any? {
            val db = ctx.getContext<GraphQLContext>().db
            val params = field.inputValueDefinitions.associate { arg -> arg.name to ctx.getArgument<Any>(arg.name) }
            return field.directives.filter { it.name == "cypher" }
                    .flatMap { cypher ->
                        cypher.arguments.filter { arg -> arg.name == "statement" }.map { it.value }.filterIsInstance<StringValue>()
                                .map { statement ->
                            val result = db.execute(statement.value, params)
                            // todo add "CypherUpdate" as typedefinition
                            if (returnType == GraphQLString) {
                                return result.queryStatistics.toString()
                            }
                            return asEntityList(db, result, returnType)
                        }
                    }.firstOrNull()
        }
        return mutations.map { field ->
            val returnType = graphqlTypeFor(field.type) as GraphQLOutputType
            GraphQLFieldDefinition.newFieldDefinition()
                    .name(field.name)
                    .type(returnType)
                    .dataFetcher { ctx -> databaseUpdate(field, ctx, returnType) }
                    .argument(field.inputValueDefinitions.map { arg ->
                        // todo directives
                        GraphQLArgument.newArgument().name(arg.name).type(graphqlTypeFor(arg.type) as GraphQLInputType).defaultValue(arg.defaultValue?.extract()).build()
                    })
                    .build()
        }
    }

    fun toGraphQLObjectType(metaData: MetaData, interfaceDefinitions: Map<String, GraphQLInterfaceType>) : GraphQLObjectType {
        var builder: GraphQLObjectType.Builder = newObject()
                .name(metaData.type)
                .description(metaData.type + "-Node")

        builder = builder.field(newFieldDefinition()
                .name("_id")
                .description("internal node id")
                //                    .fetchField().dataFetcher((env) -> null)
                .type(Scalars.GraphQLID).build())

        metaData.labels.forEach { builder = builder.withInterface(interfaceDefinitions.get(it))  }

        // todo relationships, labels etc.

        // something is off with rule-checking probably interface names conflicting with object names
        //        builder = addInterfaces(builder);
        builder = addProperties(metaData, builder)
        builder = addRelationships(metaData, builder)
        return builder.build()
    }


    fun toGraphQLInterfaceType(metaData: MetaData, typeResolver: (String?) -> GraphQLObjectType? ): GraphQLInterfaceType {
        val interfaceName = metaData.type
        var builder: GraphQLInterfaceType.Builder = GraphQLInterfaceType.newInterface()
                .name(interfaceName)
                .description(interfaceName + "-Node")

        builder = builder.field(newFieldDefinition()
                .name("_id")
                .description("internal node id")
                //                    .fetchField().dataFetcher((env) -> null)
                .type(Scalars.GraphQLID).build())

        // todo relationships, labels etc.

        // something is off with rule-checking probably interface names conflicting with object names
        //        builder = addInterfaces(builder);
        builder = addProperties(metaData, builder)
        builder = addRelationships(metaData, builder)

        builder = builder.typeResolver { cypherResult ->
            val row = cypherResult as Map<String, Any>?
            val allLabels = row?.get("_labels") as List<String>?
            // we also have to add the interfaces to the mutation on create
            val firstRemainingLabel: String? = allLabels?.filterNot { it == interfaceName }?.firstOrNull() // would be good to have a "Node" superlabel? like Relay has

            if(firstRemainingLabel.isNullOrEmpty()) {
                var builder: GraphQLObjectType.Builder = GraphQLObjectType.newObject()
                        .name(interfaceName)
                        .description(interfaceName + "-Node")

                builder = builder.field(newFieldDefinition()
                        .name("_id")
                        .description("internal node id")
                        .type(Scalars.GraphQLID).build())

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
            newBuilder = newBuilder.field(newReferenceField(md, key, info.label, info.multi, info.parameters?.values))
        }
        return newBuilder
    }

    private fun addRelationships(md: MetaData, builder: GraphQLInterfaceType.Builder): GraphQLInterfaceType.Builder {
        var newBuilder = builder
        for ((key, info) in md.relationships) {
            newBuilder = newBuilder.field(newReferenceField(md, key, info.label, info.multi, info.parameters?.values))
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

    private fun newReferenceField(md: MetaData, name: String, label: String, multi: Boolean, parameters: Iterable<MetaData.ParameterInfo>? = emptyList()): GraphQLFieldDefinition {
        val labelMd = GraphSchemaScanner.getMetaData(label)!!
        val graphQLType: GraphQLOutputType = if (multi) GraphQLList(GraphQLTypeReference(label)) else GraphQLTypeReference(label)
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
                .description(md.type + " " + name + " " + label)
                .argument(propertiesAsArguments(labelMd))
                .argument(propertiesAsListArguments(labelMd))
                .argument(orderByArgument(labelMd))
                .argument(toArguments(parameters))
                .type(graphQLType)

        return if (multi) {
            withFirstOffset(field).build()
        } else field.build()
    }

    private fun toArguments(parameters: Iterable<MetaData.ParameterInfo>?) = parameters?.map { newArgument().name(it.name).type(graphQlInType(it.type)).defaultValue(it.defaultValue).build() } ?: emptyList()

    private fun withFirstOffset(field: GraphQLFieldDefinition.Builder) = field
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
                .description(name + " of  " + md.type)
                //                      .type(ids.contains(name) ? Scalars.GraphQLID : graphQlType(value.getClass()))
                //                      .fetchField().dataFetcher((env) -> null)
                .type(graphQlOutType(prop.type))
                .argument(toArguments(prop.parameters?.values))
                .build()
    }
    
    companion object {
        class GraphQLSchemaWithDirectives(queryType: GraphQLObjectType, mutationType: GraphQLObjectType, dictionary: Set<GraphQLType>, newDirectives : List<GraphQLDirective>)
            : GraphQLSchema(queryType, mutationType, dictionary) {

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

            val myBuilder = GraphQLSchemaBuilder()

            val metaDatas = GraphSchemaScanner.allMetaDatas()
            val typeMetaDatas = metaDatas.filterNot {  it.isInterface }

            val dictionary = myBuilder.graphQlTypes(metaDatas)

            val interfaceTypes = dictionary.first
            val objectTypes = dictionary.second
            val allTypes = (objectTypes.values + interfaceTypes.values).toSet()

            val queryType = newObject().name("QueryType")
                    .fields(myBuilder.queryFields(metaDatas,allTypes))
                    .build()

            val mutationsFromSchema = GraphSchemaScanner.schema?.
                    let { IDLParser.parseMutations(it) }?.
                    let { parsedMutations -> myBuilder.toMutationFields(parsedMutations, objectTypes) } ?: emptyList()
            val mutationFields = GraphSchemaScanner.allMetaDatas()
                    .flatMap { myBuilder.relationshipMutationFields(it) + myBuilder.mutationField(it) } + mutationsFromSchema

            val mutationType: GraphQLObjectType = newObject().name("MutationType")
                    .fields(mutationFields)
                    .build()

            // todo this was missing, it was only called by the builder: SchemaUtil().replaceTypeReferences(graphQLSchema)

            val schema = GraphQLSchema.Builder().mutation(mutationType).query(queryType).build(allTypes) // interfaces seem to be quite tricky

            return GraphQLSchemaWithDirectives(schema.queryType, schema.mutationType, schema.dictionary, graphQLDirectives())
        }

        private fun graphQLDirectives() = listOf<GraphQLDirective>(
                newFieldDirective("relation", "Relationship"),
                newFieldDirective("cypher", "Cypher query to run"),
                newDirective("profile", "Enable query profiling"),
                newDirective("explain", "Enable query explanation"),
                newDirective("compile", "Enable query compilation"),
                newDirective("version", "Specify Cypher version", GraphQLArgument("version","Cypher Version (3.0, 3.1, 3.2)", GraphQLString, DEFAULT_CYPHER_VERSION))
        )

        private fun newDirective(name: String, desc: String, vararg arguments: GraphQLArgument) =
                GraphQLDirective(name, desc, EnumSet.of(Introspection.DirectiveLocation.QUERY), listOf(*arguments), true, false, true)

        private fun newFieldDirective(name: String, desc: String, vararg arguments: GraphQLArgument) =
                GraphQLDirective(name, desc, EnumSet.of(Introspection.DirectiveLocation.FIELD), listOf(*arguments), true, false, true)
    }
    private fun graphQlOutType(type: MetaData.PropertyType): GraphQLOutputType {
        var outType : GraphQLOutputType = graphQLType(type)
        if (type.array) {
            outType = GraphQLList(outType)
        }
        if (type.nonNull)
            outType = GraphQLNonNull(outType)
        return outType
    }

    private fun graphQLType(type: MetaData.PropertyType): GraphQLScalarType {
        return when (type.name) {
            "String" -> GraphQLString
            "ID" -> GraphQLID
            "Boolean" -> GraphQLBoolean
            "Number" -> GraphQLFloat
            "Float" -> GraphQLFloat
            "Int" -> GraphQLLong
            else -> throw IllegalArgumentException("Unknown field type " + type)
        }
    }

    private fun graphQlInType(type: MetaData.PropertyType, required: Boolean = true): GraphQLInputType {
        var inType : GraphQLInputType = graphQLType(type)
        if (type.array) {
            inType = GraphQLList(inType)
        }
        if (type.nonNull && required)
            inType = GraphQLNonNull(inType)
        return inType
    }

    fun queryFields(metaDatas: Iterable<MetaData>, objectTypes: Set<GraphQLType>): List<GraphQLFieldDefinition> {
        return metaDatas
                .map { md ->
                    withFirstOffset(
                            newFieldDefinition()
                            .name(md.type)
                            .type(GraphQLList(objectTypes.filter { it.name == md.type }.firstOrNull()))
                            .argument(propertiesAsArguments(md))
                            .argument(propertiesAsListArguments(md))
                            .argument(orderByArgument(md))
                            .dataFetcher({ env -> fetchGraphData(md, env) })
                    ).build()
                }
    }

    fun mutationField(metaData: MetaData) : List<GraphQLFieldDefinition> {
        val idProperty = idProperty(metaData)

        val updatableProperties = metaData.properties.values.filter { !it.isComputed() }

        class CreateMutationDataFetcher(metaData: MetaData) : DataFetcher<String> {
            val statement = "CREATE (node:${metaData.type}) SET node = {properties} " + metaData.labels.map { "SET node:`$it`" }.joinToString(", ")

            override fun get(env: DataFetchingEnvironment): String {
                val params = mapOf<String, Any>("properties" to updatableProperties.associate { it.fieldName to env.getArgument<Any>(it.fieldName) })
                return executeStatement(env, statement, params)
            }

            override fun toString(): String {
                return statement
            }
        }
        val createMutation = GraphQLFieldDefinition.newFieldDefinition()
                .name("create" + metaData.type)
                .description("Creates a ${metaData.type} entity")
                .type(GraphQLString)
                .argument(updatableProperties.map { GraphQLArgument(it.fieldName, graphQlInType(it.type)) })
                .dataFetcher(CreateMutationDataFetcher(metaData))
                .build()

        if (idProperty == null)
            return listOf(createMutation)
        else {
            val nonIdProperties = updatableProperties.filter { !it.isId() }

            return listOf(
                createMutation
        ,
                GraphQLFieldDefinition.newFieldDefinition()
                        .name("update" + metaData.type)
                        .description("Updates a ${metaData.type} entity")
                        .type(GraphQLString)
                        .argument(GraphQLArgument(idProperty.fieldName, graphQlInType(idProperty.type)))
                        .argument(nonIdProperties.map { GraphQLArgument(it.fieldName, graphQlInType(it.type)) })
                        .dataFetcher { env ->
                            val params = mapOf<String,Any>(
                                    "id" to env.getArgument<Any>(idProperty.fieldName),
                                    "properties" to nonIdProperties.associate { it.fieldName to env.getArgument<Any>(it.fieldName) })

                            val statement = "MATCH (node:`${metaData.type}` {`${idProperty.fieldName}`:{id}}) SET node += {properties}"

                            executeStatement(env, statement,params)
                        }
                        .build()
                    ,
                GraphQLFieldDefinition.newFieldDefinition()
                        .name("delete" + metaData.type)
                        .description("Deletes a ${metaData.type} entity")
                        .type(GraphQLString)
                        .argument(GraphQLArgument(idProperty.fieldName, graphQlInType(idProperty.type)))
                        .dataFetcher { env ->
                            val params = mapOf<String,Any>("id" to env.getArgument<Any>(idProperty.fieldName))

                            val statement = "MATCH (node:`${metaData.type}` {`${idProperty.fieldName}`:{id}}) DETACH DELETE node"

                            executeStatement(env, statement,params)

                        }
                        .build()

        )
        }

    }
    fun idProperty(md: MetaData) : MetaData.PropertyInfo? = md.properties.values.firstOrNull { it.isId() }

    fun relationshipMutationFields(metaData: MetaData) : List<GraphQLFieldDefinition> {
        val idProperty = idProperty(metaData)
        return  metaData.relationships.values.flatMap {  rel ->
            val targetMeta = GraphSchemaScanner.getMetaData(rel.label)!!
            val targetIdProperty = idProperty(targetMeta)
            if (idProperty == null || targetIdProperty == null) emptyList()
            else {
                val sourceArgument = GraphQLArgument(idProperty.fieldName, graphQlInType(idProperty.type))
                val targetArguments = GraphQLArgument(rel.fieldName, GraphQLNonNull(GraphQLList(graphQlInType(targetIdProperty.type))))
                val (left,right) = if (rel.out) Pair("",">") else Pair("<","")
                listOf(
                GraphQLFieldDefinition.newFieldDefinition()
                        .name("add" + metaData.type + rel.fieldName.capitalize())
                        .description("Adds ${rel.fieldName.capitalize()} to ${metaData.type} entity")
                        .type(GraphQLString)
                        .argument(sourceArgument).argument(targetArguments)
                        .dataFetcher { env ->
                            val statement = """MATCH (from:`${metaData.type}` {`${idProperty.fieldName}`:{source}})
                                               UNWIND {targets} AS target
                                               MATCH (to:`${targetMeta.type}` { `${targetIdProperty.fieldName}`: target})
                                               MERGE (from)$left-[:`${rel.type}`]-$right(to)"""

                            val params = mapOf<String,Any>("source" to env.getArgument<Any>(idProperty.fieldName), "targets" to  env.getArgument<Any>(rel.fieldName))
                            executeStatement(env, statement, params)
                        }
                        .build(),
                GraphQLFieldDefinition.newFieldDefinition()
                        .name("delete" + metaData.type + rel.fieldName.capitalize())
                        .description("Deletes ${rel.fieldName.capitalize()} from ${metaData.type} entity")
                        .type(GraphQLString)
                        .argument(sourceArgument).argument(targetArguments)
                        .dataFetcher { env ->
                            val statement = """MATCH (from:`${metaData.type}` {`${idProperty.fieldName}`:{source}})
                                               UNWIND {targets} AS target
                                               MATCH (from)$left-[rel:`${rel.type}`]-$right(to:`${targetMeta.type}` { `${targetIdProperty.fieldName}`: target})
                                               DELETE rel
                                            """

                            val params = mapOf<String,Any>("source" to env.getArgument<Any>(idProperty.fieldName), "targets" to  env.getArgument<Any>(rel.fieldName))
                            executeStatement(env, statement, params)
                        }
                        .build()
                )
            }
        }
    }

    private fun executeStatement(env: DataFetchingEnvironment, statement: String, params: Map<String, Any>): String {
        val db = env.getContext<GraphQLContext>().db

        val result = db.execute(statement, params)
        return result.queryStatistics.toString()
    }


    private fun fetchGraphData(md: MetaData, env: DataFetchingEnvironment): List<Map<String, Any>> {
        val ctx = env.getContext<GraphQLContext>()
        val db = ctx.db
        val generator = CypherGenerator.instance()
        return env.fields
                .map { it to generator.generateQueryForField(it) }
                .flatMap({ pair ->
                    val (field, query) = pair
                    val directives = field.directives.associate { it.name to it }
                    val statement = applyDirectivesToStatement(generator, query, directives)
                    println(statement)
                    val parameters = env.arguments
                    val result = db.execute(statement, parameters)
                    val list = Iterators.asList(result)
                    storeResultMetaData(ctx, query, result, directives)
                    list
                })
    }

    private fun applyDirectivesToStatement(generator: CypherGenerator, query: String, directives: Map<String, Directive>) :String {
        val parts = mutableListOf<String>()
//        if (directives.containsKey("cypher"))  { parts.add(directives.get("cypher").arguments.first().value.toString())  }
        if (directives.containsKey("compile")) { parts.add("runtime="+generator.compiled())  }
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
                .type(GraphQLList(GraphQLEnumType("_${md.type}Ordering","Ordering Enum for ${md.type}",
                        md.properties.keys.flatMap { listOf(
                                GraphQLEnumValueDefinition(it+"_asc","Ascending sort for $it",Pair(it,true)),
                                GraphQLEnumValueDefinition(it+"_desc","Descending sort for $it",Pair(it,false))) }))).build()
    }
    internal fun propertiesAsListArguments(md: MetaData): List<GraphQLArgument> {
        return md.properties.values.map {
            newArgument().name(it.fieldName+"s").description(it.fieldName + "s is list variant of "+it.fieldName + " of " + md.type).type(GraphQLList(graphQlInType(it.type, false))).build()
        }
    }
}
