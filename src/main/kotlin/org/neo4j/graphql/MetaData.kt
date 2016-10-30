package org.neo4j.graphql

import graphql.Scalars
import graphql.Scalars.GraphQLString
import graphql.language.Field
import graphql.language.IntValue
import graphql.language.StringValue
import graphql.language.VariableReference
import graphql.schema.*
import graphql.schema.GraphQLArgument.newArgument
import graphql.schema.GraphQLFieldDefinition.newFieldDefinition
import graphql.schema.GraphQLObjectType.newObject
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.helpers.collection.Iterators
import java.util.*

/**
 * @author mh
 * @since 30.10.16
 */
class MetaData {

    private var type = ""
    private val ids = LinkedHashSet<String>()
    private val indexed = LinkedHashSet<String>()
    private val properties = LinkedHashMap<String, Class<*>>()
    private val labels = LinkedHashSet<String>()
    @JvmField val relationships: MutableMap<String, RelationshipInfo> = LinkedHashMap()

    constructor(db: GraphDatabaseService, label: Label) {
        this.type = label.name()
        inspectIndexes(db, label)
        sampleNodes(db, label)
    }

    override fun toString(): String {
        return "MetaData{type='$type', ids=$ids, indexed=$indexed, properties=$properties, labels=$labels, relationships=$relationships}"
    }


    private fun inspectIndexes(db: GraphDatabaseService, label: Label) {
        for (index in db.schema().getIndexes(label)) {
            for (s in index.propertyKeys) {
                if (index.isConstraintIndex) ids.add(s)
                indexed.add(s)
            }
        }
    }

    private fun sampleNodes(db: GraphDatabaseService, label: Label) {
        var count = 10
        val nodes = db.findNodes(label)
        val values = LinkedHashMap<String, Any>()
        while (nodes.hasNext() && count-- > 0) {
            val node = nodes.next()
            for (l in node.labels) labels.add(l.name())
            values.putAll(node.allProperties)
            sampleRelationships(node)
        }
        values.forEach { k, v -> properties.put(k, v.javaClass) }
        labels.remove(type)
    }


    private fun sampleRelationships(node: Node) {
        val dense = node.degree > DENSE_NODE
        for (type in node.relationshipTypes) {
            val itOut = node.getRelationships(Direction.OUTGOING, type).iterator()
            val out = Iterators.firstOrNull(itOut)
            val typeName = type.name()
            if (out != null) {
                if (!dense || node.getDegree(type, Direction.OUTGOING) < DENSE_NODE) {
                    val outName = typeName + "_"
                    labelsFor(out.endNode) { label ->
                        this.relationships.getOrPut(outName + label)
                            { RelationshipInfo(typeName, label, true) }
                            .update(itOut.hasNext())
                    }
                }
            }
            val itIn = node.getRelationships(Direction.INCOMING, type).iterator()
            val `in` = Iterators.firstOrNull(itIn)
            if (`in` != null) {
                if (!dense || node.getDegree(type, Direction.INCOMING) < DENSE_NODE) {
                    val inName = "_" + typeName
                    labelsFor(`in`.startNode) { label ->
                        this.relationships.getOrPut(label + inName)
                            { RelationshipInfo(typeName, label, false) }
                            .update(itIn.hasNext())
                    }
                }
            }
        }
    }

    private fun labelsFor(node: Node, consumer: (String) -> Unit ) {
        for (label in node.labels) {
            consumer.invoke(label.name())
        }
    }


    fun toGraphQLInterface(): GraphQLInterfaceType {
        var builder: GraphQLInterfaceType.Builder = GraphQLInterfaceType.newInterface()
                .typeResolver { `object` -> allTypes[`object`.toString()]!!.toGraphQL() }
                .name(type)
                .description(type + "-Label")
        // it uses the interface types for checking against parent-types of fields, which is weird
        builder = addProperties(builder, ids, indexed) // mostly id, indexed, not sure about others
        return builder.build()
    }

    private fun addProperties(builder: GraphQLInterfaceType.Builder, ids: Set<String>, indexed: Set<String>): GraphQLInterfaceType.Builder {
        var newBuilder = builder
        for (key in indexed) {
            newBuilder = newBuilder.field(newField(key, properties[key]!!))
        }
        return newBuilder
    }

    fun toGraphQL(): GraphQLObjectType {
        var builder: GraphQLObjectType.Builder = newObject()
                .name(type)
                .description(type + "-Node")

        builder = builder.field(newFieldDefinition()
                .name("_id")
                .description("internal node id")
                //                    .fetchField().dataFetcher((env) -> null)
                .type(Scalars.GraphQLID).build())


        // todo relationships, labels etc.

        // something is off with rule-checking probably interface names conflicting with object names
        //        builder = addInterfaces(builder);
        builder = addProperties(builder)
        builder = addRelationships(builder)
        return builder.build()
    }

    private fun addRelationships(builder: GraphQLObjectType.Builder): GraphQLObjectType.Builder {
        var newBuilder = builder
        for ((key, info) in relationships) {
            newBuilder = newBuilder.field(newReferenceField(key, info.label, info.multi))
        }
        return newBuilder
    }

    private fun addProperties(builder: GraphQLObjectType.Builder): GraphQLObjectType.Builder {
        var newBuilder = builder
        for ((name, type) in properties) {
            newBuilder = newBuilder.field(newField(name, type))
        }
        return newBuilder
    }

    private fun newReferenceField(name: String, label: String, multi: Boolean): GraphQLFieldDefinition {
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
                .description(this.type + " " + name + " " + label)
                .type(if (multi) GraphQLList(GraphQLTypeReference(label)) else GraphQLTypeReference(label))
                .build()
    }

    private fun newField(name: String, type: Class<*>): GraphQLFieldDefinition {
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
                .description(name + " of  " + this.type)
                //                      .type(ids.contains(name) ? Scalars.GraphQLID : graphQlType(value.getClass()))
                //                      .fetchField().dataFetcher((env) -> null)
                .type(graphQlOutType(type))
                .build()
    }

    private fun addInterfaces(builder: GraphQLObjectType.Builder): GraphQLObjectType.Builder {
        var newBuilder = builder
        for (label in labels) {
            newBuilder = newBuilder.withInterface(allTypes[label]!!.toGraphQLInterface())
        }
        return newBuilder
    }


    companion object {

        val DENSE_NODE = 50
        internal val allTypes = LinkedHashMap<String, MetaData>()

        @JvmStatic fun from(db: GraphDatabaseService, label: Label): MetaData {
            return MetaData(db, label)
        }

        @JvmStatic fun buildSchema(db: GraphDatabaseService): GraphQLSchema {
            databaseSchema(db)

            var schema = GraphQLSchema.Builder()

            val mutationType: GraphQLObjectType? = null
            //        schema = schema.mutation(mutationType);

            val queryType = newObject().name("QueryType").fields(queryFields()).build()
            schema = schema.query(queryType)

            return schema.build(graphQlTypes())
        }

        private fun databaseSchema(db: GraphDatabaseService) {
            allTypes.clear()

            val tx = db.beginTx()
            try {
                for (label in db.allLabels) {
                    allTypes.put(label.name(), from(db, label))
                }
                tx.success()
            } finally {
                tx.close()
            }
/*todo
            db.beginTx().use { tx :Transaction ->
                tx.success()
            }
*/
        }

        fun getAllTypes(): Map<String, MetaData> {
            return allTypes
        }

        private fun graphQlOutType(type: Class<*>): GraphQLOutputType {
            if (type == String::class.java) return GraphQLString
            if (Number::class.java.isAssignableFrom(type)) {
                if (type == Double::class.java || type == Float::class.java) return Scalars.GraphQLFloat
                return Scalars.GraphQLLong
            }
            if (type == Boolean::class.java) return Scalars.GraphQLBoolean
            if (type.javaClass.isArray) {
                return GraphQLList(graphQlOutType(type.componentType))
            }
            throw IllegalArgumentException("Unknown field type " + type)
        }

        private fun graphQlInType(type: Class<*>): GraphQLInputType {
            if (type == String::class.java) return GraphQLString
            if (Number::class.java.isAssignableFrom(type)) {
                if (type == Double::class.java || type == Float::class.java) return Scalars.GraphQLFloat
                return Scalars.GraphQLLong
            }
            if (type == Boolean::class.java) return Scalars.GraphQLBoolean
            if (type.javaClass.isArray) {
                return GraphQLList(graphQlInType(type.componentType))
            }
            throw IllegalArgumentException("Unknown field type " + type)
        }

        fun queryFields(): List<GraphQLFieldDefinition> {
            return allTypes.values
                    .map{ md ->
                        newFieldDefinition()
                                .name(md.type)
                                .type(GraphQLList(md.toGraphQL()))
                                .argument(md.propertiesAsArguments())
                                //                            .fetchField();
                                .dataFetcher({ env -> fetchGraphData(md, env) }).build()
                    }
        }

        private fun fetchGraphData(md: MetaData, env: DataFetchingEnvironment): List<Map<String, Any>> {
            val db = env.context as GraphDatabaseService
            return env.fields
                    .map { generateQueryForField(it) }
                    .flatMap({ query ->
                        val parameters = env.arguments
                        val result = db.execute(query, parameters)
                        Iterators.asList(result)
                    })
        }

        private fun generateQueryForField(field: Field): String {
            val name = field.name
            val variable = name
            val metaData = allTypes[name]!!


            var query = String.format("MATCH (%s:`%s`) \n", variable, name)
            query += addWhere(field, variable)
            for (selection in field.selectionSet.selections) {
                if (selection is Field) {
                    val fieldName = selection.name
                    val info = metaData.relationships[fieldName]
                    if (info != null) {
                        query += String.format(" OPTIONAL MATCH (`%s`)%s-[:`%s`]-%s(`%s`:`%s`) \n", variable, if (!info.out) "<" else "", info.type, if (info.out) ">" else "", fieldName, info.label)
                        // todo handle conditions on related elements after optional match
                        //TODO                    query += addWhere(field, variable);
                    }
                }
            }
            query += " RETURN "
            query += selectionFields(field, variable, metaData, false)
            println("query = " + query)
            return query
        }

        private fun addWhere(field: Field, variable: String): String {
            if (field.arguments.isEmpty()) {
                return ""
            }
            var query = " WHERE "
            val args = field.arguments.iterator()
            while (args.hasNext()) {
                val argument = args.next()
                query += String.format("%s.`%s` = ", variable, argument.name)
                val value = argument.value
                if (value is VariableReference) query += String.format("{`%s`}", value.name)
                // todo turn into parameters
                if (value is IntValue) query += value.value
                if (value is StringValue) query += "\"" + value.value + "\""
                if (args.hasNext()) query += " AND \n"
            }
            return query
        }

        private fun selectionFields(field: Field, variable: String, metaData: MetaData, prefix: Boolean): String {
            val selectionSet = field.selectionSet ?: return ""

            val it = selectionSet.selections.iterator()
            var query = ""
            while (it.hasNext()) {
                val f = it.next() as Field
                val fieldName = f.name
                val alias = if (f.alias != null) f.alias else fieldName
                val info = metaData.relationships[fieldName] // todo correct medatadata of
                if (prefix) {
                    query += alias + ": "
                }
                if (info == null) {
                    query += String.format("`%s`.`%s`", variable, fieldName)
                } else {
                    if (f.selectionSet != null) {
                        val subFields = selectionFields(f, f.name, allTypes[info.label]!!, true)
                        query += String.format(if (info.multi) "collect({%s})" else "{%s}", subFields)
                    }
                }
                if (!prefix) {
                    query += " AS " + alias
                }
                if (it.hasNext()) query += ",\n"
            }
            return query
        }

        fun graphQlTypes(): Set<GraphQLType> {
            return allTypes.values.map { it.toGraphQL() }.toSet()
        }
    }

    internal fun propertiesAsArguments(): List<GraphQLArgument> {
        return properties.entries.map{
            newArgument().name(it.key).description(it.key + " of " + type).type(graphQlInType(it.value)).build()
        }
    }
}
