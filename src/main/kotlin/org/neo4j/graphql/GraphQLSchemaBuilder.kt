package org.neo4j.graphql

import graphql.Scalars
import graphql.Scalars.GraphQLString
import graphql.schema.*
import graphql.schema.GraphQLArgument.newArgument
import graphql.schema.GraphQLFieldDefinition.newFieldDefinition
import graphql.schema.GraphQLObjectType.newObject
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.helpers.collection.Iterators

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
                .type(if (multi) GraphQLList(GraphQLTypeReference(label)) else GraphQLTypeReference(label))
                .build()
    }

    private fun newField(md: MetaData, name: String, type: Class<*>): GraphQLFieldDefinition {
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
        @JvmStatic fun buildSchema(db: GraphDatabaseService): GraphQLSchema {
            GraphSchemaScanner.databaseSchema(db)

            var schema = GraphQLSchema.Builder()

            val builder = GraphQLSchemaBuilder()



            val mutationType: GraphQLObjectType? = null
            //        schema = schema.mutation(mutationType);

            val queryType = newObject().name("QueryType").fields(builder.queryFields(GraphSchemaScanner.allMetaDatas())).build()
            schema = schema.query(queryType)

            return schema.build(builder.graphQlTypes(GraphSchemaScanner.allMetaDatas()))
        }
    }
    private fun graphQlOutType(type: Class<*>): GraphQLOutputType {
        if (type == String::class.java) return GraphQLString
        if (Number::class.java.isAssignableFrom(type)) {
            if (type == Double::class.java || type == Double::class.javaObjectType || type == Float::class.java || type == Float::class.javaObjectType) return Scalars.GraphQLFloat
            return Scalars.GraphQLLong
        }
        if (type == Boolean::class.java || type == Boolean::class.javaObjectType) return Scalars.GraphQLBoolean
        if (type.javaClass.isArray) {
            return GraphQLList(graphQlOutType(type.componentType))
        }
        throw IllegalArgumentException("Unknown field type " + type)
    }

    private fun graphQlInType(type: Class<*>): GraphQLInputType {
        if (type == String::class.java) return GraphQLString
        if (Number::class.java.isAssignableFrom(type)) {
            if (type == Double::class.java || type == Double::class.javaObjectType || type == Float::class.java || type == Float::class.javaObjectType) return Scalars.GraphQLFloat
            return Scalars.GraphQLLong
        }
        if (type == Boolean::class.java || type == Boolean::class.javaObjectType) return Scalars.GraphQLBoolean
        if (type.javaClass.isArray) {
            return GraphQLList(graphQlInType(type.componentType))
        }
        throw IllegalArgumentException("Unknown field type " + type)
    }

    fun queryFields(metaDatas: Iterable<MetaData>): List<GraphQLFieldDefinition> {
        return metaDatas
                .map { md ->
                    newFieldDefinition()
                            .name(md.type)
                            .type(GraphQLList(toGraphQL(md)))
                            .argument(propertiesAsArguments(md))
                            .argument(propertiesAsListArguments(md))
                            //                            .fetchField();
                            .dataFetcher({ env -> fetchGraphData(md, env) }).build()
                }
    }

    private fun fetchGraphData(md: MetaData, env: DataFetchingEnvironment): List<Map<String, Any>> {
        val db = env.context as GraphDatabaseService
        return env.fields
                .map { Cypher30Generator().generateQueryForField(it) }
                .flatMap({ query ->
                    val parameters = env.arguments
                    val result = db.execute(query, parameters)
                    Iterators.asList(result)
                })
    }

    fun graphQlTypes(metaDatas: Iterable<MetaData>): Set<GraphQLType> {
        return metaDatas.map { toGraphQL(it) }.toSet()
    }

    internal fun propertiesAsArguments(md: MetaData): List<GraphQLArgument> {
        return md.properties.entries.map {
            newArgument().name(it.key).description(it.key + " of " + md.type).type(graphQlInType(it.value)).build()
        }
    }
    internal fun propertiesAsListArguments(md: MetaData): List<GraphQLArgument> {
        return md.properties.entries.map {
            newArgument().name(it.key+"s").description(it.key + "s is list variant of "+it.key + " of " + md.type).type(GraphQLList(graphQlInType(it.value))).build()
        }
    }
}
