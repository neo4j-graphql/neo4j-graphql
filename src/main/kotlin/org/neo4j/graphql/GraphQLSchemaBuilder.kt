package org.neo4j.graphql

import graphql.Scalars
import graphql.Scalars.*
import graphql.introspection.Introspection
import graphql.language.Directive
import graphql.schema.*
import graphql.schema.GraphQLArgument.newArgument
import graphql.schema.GraphQLFieldDefinition.newFieldDefinition
import graphql.schema.GraphQLObjectType.newObject
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result
import org.neo4j.graphql.CypherGenerator.Companion.DEFAULT_CYPHER_VERSION
import org.neo4j.helpers.collection.Iterators
import java.util.*

class GraphQLSchemaBuilder {

    fun toGraphQLInterface(md: MetaData): GraphQLInterfaceType {
        var builder: GraphQLInterfaceType.Builder = GraphQLInterfaceType.newInterface()
                .typeResolver { toGraphQL(GraphSchemaScanner.getMetaData(it.toString())!!) }
                .name(md.type)
                .description(md.type + "-Label")
        // it uses the interface types for checking against parent-types of fields, which is weird
        builder = addInterfaceProperties(md, builder)
        return builder.build()
    }

    private fun addInterfaceProperties(md: MetaData, builder: GraphQLInterfaceType.Builder): GraphQLInterfaceType.Builder {
        // mostly id, indexed, not sure about others
        // ids: Set<String>, indexed: Set<String>
        var newBuilder = builder
        for (key in md.indexed) {
            newBuilder = newBuilder.field(newField(md, key, md.properties[key]!!))
        }
        return newBuilder
    }

    fun toGraphQL(metaData: MetaData): GraphQLObjectType {
        var builder: GraphQLObjectType.Builder = newObject()
                .name(metaData.type)
                .description(metaData.type + "-Node")

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
        return builder.build()
    }

    private fun addRelationships(md: MetaData, builder: GraphQLObjectType.Builder): GraphQLObjectType.Builder {
        var newBuilder = builder
        for ((key, info) in md.relationships) {
            newBuilder = newBuilder.field(newReferenceField(md, key, info.label, info.multi))
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

    private fun newReferenceField(md: MetaData, name: String, label: String, multi: Boolean): GraphQLFieldDefinition {
        val labelMd = GraphSchemaScanner.getMetaData(label)!!
        val graphQLType: GraphQLOutputType = if (multi) GraphQLList(GraphQLTypeReference(label)) else GraphQLTypeReference(label)
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
                .description(md.type + " " + name + " " + label)
                .argument(propertiesAsArguments(labelMd))
                .argument(propertiesAsListArguments(labelMd))
                .argument(orderByArgument(labelMd))
                .type(graphQLType)
                .build()
    }

    private fun newField(md: MetaData, name: String, type: MetaData.PropertyType): GraphQLFieldDefinition {
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
                .type(graphQlOutType(type))
                .build()
    }

    private fun addInterfaces(md: MetaData, builder: GraphQLObjectType.Builder): GraphQLObjectType.Builder {
        var newBuilder = builder
        for (label in md.labels) {
            val metaData = GraphSchemaScanner.getMetaData(label)!!
            newBuilder = newBuilder.withInterface(toGraphQLInterface(metaData))
        }
        return newBuilder
    }

    companion object {
        class GraphQLSchemaWithDirectives(queryType: GraphQLObjectType, mutationType: GraphQLObjectType, dictionary: Set<GraphQLType>, newDirectives : List<GraphQLDirective>)
            : GraphQLSchema(queryType, mutationType, dictionary) {

            val myDirectives : List<GraphQLDirective>
            init {
                this.myDirectives = newDirectives + super.getDirectives();
            }
            override fun getDirectives(): List<GraphQLDirective> {
                return myDirectives
            }
        }

        @JvmStatic fun buildSchema(db: GraphDatabaseService): GraphQLSchema {
            GraphSchemaScanner.databaseSchema(db)

            val myBuilder = GraphQLSchemaBuilder()

            val mutationFields = GraphSchemaScanner.allMetaDatas().flatMap { myBuilder.relationshipMutationFields(it) + myBuilder.mutationField(it) }

            val mutationType: GraphQLObjectType = newObject().name("MutationType")
                    .fields(mutationFields)
                    .build()

            val queryType = newObject().name("QueryType")
                    .fields(myBuilder.queryFields(GraphSchemaScanner.allMetaDatas()))
                    .build()

            val dictionary = myBuilder.graphQlTypes(GraphSchemaScanner.allMetaDatas())

            // todo this was missing, it was only called by the builder: SchemaUtil().replaceTypeReferences(graphQLSchema)
            val schema = GraphQLSchema.Builder().mutation(mutationType).query(queryType).build(dictionary)
            return GraphQLSchemaWithDirectives(schema.queryType, schema.mutationType, schema.dictionary, graphQLDirectives())
        }

        private fun graphQLDirectives() = listOf<GraphQLDirective>(
                newFieldDirective("in", "Relationship coming in"),
                newFieldDirective("out", "Relationship going out"),
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
            "Boolean" -> GraphQLBoolean
            "Number" -> GraphQLFloat
            "Float" -> GraphQLFloat
            "Int" -> GraphQLLong
            else -> throw IllegalArgumentException("Unknown field type " + type)
        }
    }

    private fun graphQlInType(type: MetaData.PropertyType): GraphQLInputType {
        var inType : GraphQLInputType = graphQLType(type)
        if (type.array) {
            inType = GraphQLList(inType)
        }
        if (type.nonNull)
            inType = GraphQLNonNull(inType)
        return inType
    }

    fun queryFields(metaDatas: Iterable<MetaData>): List<GraphQLFieldDefinition> {
        return metaDatas
                .map { md ->
                    newFieldDefinition()
                            .name(md.type)
                            .type(GraphQLList(toGraphQL(md)))
                            .argument(propertiesAsArguments(md))
                            .argument(propertiesAsListArguments(md))
                            .argument(orderByArgument(md))
                            //                            .fetchField();
                            .dataFetcher({ env -> fetchGraphData(md, env) }).build()
                }
    }

    fun mutationField(metaDatas: MetaData) : GraphQLFieldDefinition {
        return GraphQLFieldDefinition.newFieldDefinition()
                .name("create" + metaDatas.type)
                .description("Creates a ${metaDatas.type} entity")
                .type(GraphQLInt)
                .argument(metaDatas.properties.map { GraphQLArgument(it.key, graphQlInType(it.value)) })
                .dataFetcher { env ->
                    val statement = "CREATE (node:${metaDatas.type}) SET node = {properties}"
                    val db = env.getContext<GraphQLContext>().db

                    val params = mapOf<String,Any>("properties" to metaDatas.properties.keys.associate { it to env.getArgument<Any>(it) })

                    val result = db.execute(statement, params)
                    result.queryStatistics.nodesCreated

                }
                .build()

    }
    fun idProperty(md: MetaData) : Pair<String,MetaData.PropertyType> = md.properties.entries.filter { it.value.nonNull }.map{ it.key to it.value }.get(0)
    fun relationshipMutationFields(metaData: MetaData) : List<GraphQLFieldDefinition> {
        val idProperty = idProperty(metaData)
        return  metaData.relationships.values.map {  rel ->
            val targetMeta = GraphSchemaScanner.getMetaData(rel.label)!!
            val targetIdProperty = idProperty(targetMeta)
            GraphQLFieldDefinition.newFieldDefinition()
                    .name("add" + metaData.type+ rel.fieldName.capitalize())
                    .description("Adds rel.fieldName.capitalize() to ${metaData.type} entity")
                    .type(GraphQLInt)
                    .argument(metaData.properties.map { GraphQLArgument(idProperty.first, graphQlInType(idProperty.second)) })
                    .argument(metaData.properties.map { GraphQLArgument(rel.fieldName, GraphQLList(graphQlInType(targetIdProperty.second))) })
                    .dataFetcher { env ->
                        val statement = """MATCH (from:`${metaData.type}` {`${idProperty.first}`:{source}})
                                           UNWIND {targets} AS target
                                           MATCH (to:`${targetMeta.type}` { `${targetIdProperty.first}`: target})
                                           MERGE (from)-[:`${rel.type}`]->(to)"""

                        println("statement = ${statement}")
                        val db = env.getContext<GraphQLContext>().db

                        val params = mapOf<String,Any>("source" to env.getArgument<Any>(idProperty.first), "targets" to  env.getArgument<Any>(rel.fieldName))

                        val result = db.execute(statement, params)
                        result.queryStatistics.nodesCreated

                    }
                    .build()
        }


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
//                    println(statement)
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

        return if (parts.isEmpty()) query else parts.joinToString(" ","cypher "," ") + query;
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

    fun graphQlTypes(metaDatas: Iterable<MetaData>): Set<GraphQLType> {
        return metaDatas.map { toGraphQL(it) }.toSet()
    }

    internal fun propertiesAsArguments(md: MetaData): List<GraphQLArgument> {
        return md.properties.entries.map {
            newArgument().name(it.key).description(it.key + " of " + md.type).type(graphQlInType(it.value)).build()
        }
    }
    internal fun orderByArgument(md: MetaData): GraphQLArgument {
        return newArgument().name("orderBy")
                .type(GraphQLList(GraphQLEnumType("_${md.type}Ordering","Ordering Enum for ${md.type}",
                        md.properties.keys.flatMap { listOf(
                                GraphQLEnumValueDefinition(it+"_asc","Ascending sort for $it",Pair(it,true)),
                                GraphQLEnumValueDefinition(it+"_desc","Descending sort for $it",Pair(it,false))) }))).build();
    }
    internal fun propertiesAsListArguments(md: MetaData): List<GraphQLArgument> {
        return md.properties.entries.map {
            newArgument().name(it.key+"s").description(it.key + "s is list variant of "+it.key + " of " + md.type).type(GraphQLList(graphQlInType(it.value))).build()
        }
    }
}
