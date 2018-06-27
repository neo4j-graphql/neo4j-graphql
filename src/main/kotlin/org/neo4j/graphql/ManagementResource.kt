package org.neo4j.graphql

import graphql.ExecutionInput
import graphql.GraphQL
import graphql.Scalars
import graphql.schema.*
import org.codehaus.jackson.map.ObjectMapper
import org.neo4j.cypher.internal.compiler.v3_1.CartesianPoint
import org.neo4j.cypher.internal.compiler.v3_1.GeographicPoint
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.internal.kernel.api.procs.FieldSignature
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.kernel.configuration.Config
import org.neo4j.kernel.impl.proc.Procedures
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.Log
import org.neo4j.logging.LogProvider
import org.neo4j.procedure.Mode
import javax.ws.rs.*
import javax.ws.rs.core.Context
import javax.ws.rs.core.HttpHeaders
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

/**
 * @author mh
 * @since 30.10.16
 */
@javax.ws.rs.Path("admin")
class ManagementResource(@Context val provider: LogProvider, @Context val db: GraphDatabaseService) {
    val log: Log

    init {
        log = provider.getLog(GraphQLResource::class.java)
    }

    val readProcedureNames = setOf("db.index.explicit.existsForRelationships",
            "dbms.functions","dbms.procedures",
            "dbms.getTXMetaData","dbms.listActiveLocks","dbms.listConfig","dbms.listQueries","dbms.listTransactions",
            "dbms.security.listRoles","dbms.security.listRolesForUser","dbms.security.listUsers","dbms.security.listUsersForRole",
            "dbms.security.showCurrentUser","dbms.showCurrentUser","dbms.components",
            "dbms.cluster.role","dbms.cluster.overview","dbms.cluster.routing.getServers",
            "dbms.cluster.routing.getRoutersForDatabase","dbms.cluster.routing.getRoutersForAllDatabases")

    val schema = procedureSchema(filter("read"),filter("write"))

    private fun filter(type:String = "read") : (ProcedureSignature)->Boolean {
        val params = (db as GraphDatabaseAPI).dependencyResolver.resolveDependency(Config::class.java).getRaw()
        val filter = params["graphql.admin.procedures."+type] ?: ""
        if (filter.isEmpty()) return { false }
        // val re = filter.replace(",","|").replace("[?{}[]().]","\\\\$0").replace("*",".+").toRegex()
        val filters = filter.replace("*","").split(",").map { fieldName(it.split(".").toTypedArray()) }
        return { fieldName(it).findAnyOf(filters,0,true) != null}
    }

    enum class CypherTypes {
        Any, Boolean, Float, String, Integer,  List, Map, Point, Date, DateTime, Node, Relationship, Path;

        companion object {
            fun typeOf(v : kotlin.Any?) = when(v) {
                null -> Any
                is kotlin.Boolean -> Boolean
                is kotlin.Float -> Float
                is kotlin.Double -> Float
                is kotlin.Int -> Integer
                is kotlin.Long -> Integer
                is kotlin.String -> String
                is kotlin.collections.List<*> -> List
                is kotlin.collections.Map<*,*> -> Map
                is org.neo4j.graphdb.spatial.Point -> Point
                is org.neo4j.graphdb.Node -> Node
                is org.neo4j.graphdb.Relationship -> Relationship
                is org.neo4j.graphdb.Path -> Path
                else -> String
            }
            fun typeOf(type: kotlin.String?) = type?.let { valueOf(it) } ?: String
        }

        fun parse(value: kotlin.String): kotlin.Any? =
                when (this) {
                    Boolean -> value.toBoolean()
                    Float -> value.toDouble()
                    Integer -> value.toLong()
                    List -> OBJECT_MAPPER.readValue(value, List::class.java)
                    Map -> OBJECT_MAPPER.readValue(value, Map::class.java)
                    // todo Node, Path, Rel, Date, Geo
                    else -> value
                }
    }

    fun graphTypes() = setOf(
            GraphQLObjectType.newObject().name("PGNode").description("Graph Node")
                    .field{it.name("identity").type(GraphQLNonNull(Scalars.GraphQLID))}
                    .field{it.name("labels").type(GraphQLList(Scalars.GraphQLString))}
                    .field{it.name("properties").type(GraphQLList(GraphQLTypeReference("Attribute")))}
                    .build(),
            GraphQLObjectType.newObject().name("PGRelationship").description("Graph Relationship")
                    .field{it.name("identity").type(GraphQLNonNull(Scalars.GraphQLID))}
                    .field{it.name("type").type(Scalars.GraphQLString)}
                    .field{it.name("properties").type(GraphQLList(GraphQLTypeReference("Attribute")))}
                    .field{it.name("start").type(GraphQLNonNull(GraphQLTypeReference("PGNode")))}
                    .field{it.name("end").type(GraphQLNonNull(GraphQLTypeReference("PGNode")))}
                    .build(),
            GraphQLObjectType.newObject().name("PGPath").description("Graph Path")
                    .field{it.name("start").type(GraphQLNonNull(GraphQLTypeReference("PGNode")))}
                    .field{it.name("end").type(GraphQLNonNull(GraphQLTypeReference("PGNode")))}
                    .field{it.name("length").type(GraphQLNonNull(Scalars.GraphQLInt))}
                    .field{it.name("segments").type(GraphQLList(
                            GraphQLObjectType.newObject().name("PGPathSegment").description("Directed segment of a path")
                                    .field{it.name("start").type(GraphQLNonNull(GraphQLTypeReference("PGNode")))}
                                    .field{it.name("end").type(GraphQLNonNull(GraphQLTypeReference("PGNode")))}
                                    .field{it.name("relationship").type(GraphQLNonNull(GraphQLTypeReference("PGRelationship")))}
                                    .build()
                    ))}
                    .build()
    )


    fun procedureSchema(readFilter: (ProcedureSignature) -> Boolean = {true},writeFilter: (ProcedureSignature) -> Boolean = {true}): GraphQLSchema {
        val attributeTypes = attributeTypes()
        val procedures = (db as GraphDatabaseAPI).dependencyResolver.resolveDependency(Procedures::class.java)
        val builder = GraphQLSchema.newSchema()
        // todo filter read and write procedures properly by some sensible means
        // perhaps via user?
        // make all queries ?
        val allProcedures = procedures.allProcedures.associate { fieldName(it) to it }.toSortedMap().values
        val readProcsType = GraphQLObjectType.newObject().name("ReadProcedures").description("Read-only procedures")
                .fields(allProcedures.filter { isReadProcedure(it) }.filter(readFilter).map { this.procToField(it, attributeTypes) }).build()

        val writeProcsType = GraphQLObjectType.newObject().name("WriteProcedures").description("Write procedures")
                .fields(allProcedures.filter { it.mode() == Mode.WRITE || !isReadProcedure(it) }.filter(writeFilter).map { this.procToField(it, attributeTypes) }).build()

        return builder.query(readProcsType).mutation(writeProcsType).additionalTypes(attributeTypes.all() + graphTypes()).build()
    }

    private fun isReadProcedure(it: ProcedureSignature) =
            it.mode() == Mode.READ || (it.mode() == Mode.DBMS || it.mode() == Mode.SCHEMA || it.mode() == Mode.DEFAULT) && it.name().toString() in readProcedureNames

    data class Attributes(val types: GraphQLEnumType, val output:GraphQLObjectType, val input:GraphQLInputObjectType) {
        fun all() = setOf(types, output, input)
    }

    private fun attributeTypes(): Attributes {
        val types = GraphQLEnumType("Type","Neo4j Types", CypherTypes.values().map { it.name.let { GraphQLEnumValueDefinition(it,it,it)} })

        val attribute = GraphQLObjectType.newObject().name("Attribute")
                .field{ it.name("key").type(Scalars.GraphQLString)}
                .field{ it.name("value").type(Scalars.GraphQLString)}
                .field{ it.name("type").type(types)} // todo enum
                .build();
        val attributeInput = GraphQLInputObjectType.newInputObject().name("AttributeInput")
                .field{ it.name("key").type(Scalars.GraphQLString)}
                .field{ it.name("value").type(Scalars.GraphQLString)}
                .field{ it.name("type").type(types).defaultValue("String")}
                .build();

        return Attributes(types, attribute, attributeInput)
    }

    private fun procToField(proc: ProcedureSignature, attributeTypes: Attributes): GraphQLFieldDefinition {
        return GraphQLFieldDefinition.newFieldDefinition()
                .name(fieldName(proc)) // todo namespaces
                .description(proc.description().orElse(proc.toString()))
                .argument(proc.inputSignature().map { toArgument(it, attributeTypes.input) })
                .dataFetcher { executeProc(proc, it) } // todo completable future<List>
                .type((if (proc.isVoid) Scalars.GraphQLBoolean else GraphQLList(toOutputType(proc,attributeTypes.output))) as GraphQLOutputType)
                .build()
    }

    private fun executeProc(proc: ProcedureSignature, env: DataFetchingEnvironment) : Any {
        val arguments = env.fieldDefinition.arguments
        val mapArgs = arguments.filter { it.type.inner().name == "AttributeInput" }.associate { it.name to argToMap(env.getArgument<List<Map<String,String>>>(it.name)) }
        val passedArgNames = arguments.map { it.name }.filter(env::containsArgument)
        val args = env.arguments + mapArgs
        val result = env.getContext<GraphQLContext>().db.execute("CALL ${proc.name()}(${passedArgNames.map { "$" + it }.joinToString(",")})", args)
        return if (proc.isVoid) true else result.asSequence().map { it.mapValues { safeValue(it.value) } }.toList()
    }

    private fun argToMap(value: List<Map<String, String>>?): Map<String,Any?>? {
        return value?.filter { it["key"] !=null }?.associate { it["key"]!! to convertString(it["value"],it["type"]) }
    }

    private fun convertString(value: String?, type: String?) = value?.let { v -> CypherTypes.typeOf(type).parse(v) }



    private fun safeValue(v: Any?): Any? {
        fun id(id:Long) = mapOf("identity" to id)
        fun attributes(m:Map<*,*>) = m.map { (k,v) -> mapOf("key" to k, "value" to v.toString(), "type" to CypherTypes.typeOf(v)) }
        return when (v) {
            null -> null
            is Number -> v
            is Boolean -> v
            is Iterable<*> -> v.map { safeValue(it) }
            is Node -> mapOf("identity" to v.id, "labels" to v.labels.map { it.name() }, "properties" to attributes(v.allProperties))
            is Relationship -> mapOf("identity" to v.id, "type" to v.type.name(), "properties" to attributes(v.allProperties), "start" to id(v.startNodeId), "end" to id(v.endNodeId))
            is org.neo4j.graphdb.Path -> mapOf("length" to v.length(), "start" to safeValue(v.startNode()), "end" to safeValue(v.endNode()),
                    "segments" to v.relationships().map { mapOf("start" to safeValue(it.startNode),"end" to safeValue(it.endNode),"relationship" to safeValue(it))})
            is Map<*,*> -> attributes(v)
            is GeographicPoint -> mapOf("x" to v.x(),"y" to v.y(),"crs" to v.crs().name())
            is CartesianPoint -> mapOf("x" to v.x(),"y" to v.y(),"crs" to v.crs().name())
            else -> v.toString()
        }
    }

    private fun toArgument(field: FieldSignature, mapType: GraphQLInputObjectType) =
            GraphQLArgument.newArgument()
                    .name(field.name())
                    .description(if (field.isDeprecated) "deprecated" else "")
//                    .defaultValue(field.defaultValue().map { it.value() }.orElseGet { null }) // todo need to convert value
                    .type((toBasicType(field.neo4jType(), mapType) as GraphQLInputType).let { if (field.defaultValue().isPresent) it else GraphQLNonNull(it) })
                    .build()

    private fun toOutputType(proc: ProcedureSignature, mapType: GraphQLObjectType) =
            if (proc.isVoid) Scalars.GraphQLBoolean // todo
            else
                GraphQLObjectType.newObject()
                        .name(fieldName(proc) + "_Output")
                        .description("Output type for " + proc.name().toString())
                        .fields(proc.outputSignature().map { GraphQLFieldDefinition.newFieldDefinition().name(it.name()).type(toBasicType(it.neo4jType(), mapType) as GraphQLOutputType).build() })
                        .build()

    private fun toBasicType(neo4jType: Neo4jTypes.AnyType, mapType: GraphQLType): GraphQLType =
            when (neo4jType) {
                is Neo4jTypes.BooleanType -> Scalars.GraphQLBoolean
                is Neo4jTypes.FloatType -> Scalars.GraphQLFloat
                is Neo4jTypes.TextType -> Scalars.GraphQLString
                is Neo4jTypes.ListType -> GraphQLList(toBasicType(neo4jType.innerType(), mapType))
                is Neo4jTypes.MapType -> GraphQLList(mapType)
/* TODO
        is Neo4jTypes.MapType -> GraphObjectType(toBasicType(neo4jType).inner())
        is Neo4jTypes.NodeType ->
        is Neo4jTypes.RelationshipType ->
        is Neo4jTypes.PathType ->
        is Neo4jTypes.PointType ->
        is Neo4jTypes.GeometryType ->
        is Neo4jTypes.AnyType ->
*/
                else -> Scalars.GraphQLString
            }

    private fun fieldName(proc: ProcedureSignature) = fieldName(proc.name().namespace() + proc.name().name())

    private fun fieldName(parts: Array<String>) = parts.mapIndexed { i, s -> if (i> 0) s.capitalize() else s }.joinToString("")

    companion object {
        val OBJECT_MAPPER: ObjectMapper = ObjectMapper()
    }

    @Path("")
    @OPTIONS
    fun options(@Context headers: HttpHeaders) = Response.ok().build()

    @Path("")
    @GET
    fun get(@QueryParam("query") query: String?, @QueryParam("variables") variableParam: String?): Response {
        if (query == null) return Response.noContent().build()
        return asResponse(executeQuery(hashMapOf("query" to query, "variables" to (variableParam ?: emptyMap<String, Any?>()))))
    }

    @Path("")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    fun executeOperation(body: String): Response {
        return asResponse(executeQuery(parseMap(body)))
    }

    fun asResponse(result: Map<String, Any?>) = Response.ok().entity(formatMap(result)).build()

    fun executeQuery(params: Map<String, Any>): LinkedHashMap<String, Any?> {
        val result = linkedMapOf<String,Any?>()
        val tx = db.beginTx()
        try {
            try {
                val query = params["query"] as String
                val variables = getVariables(params)
                if (log.isDebugEnabled()) log.debug("Executing {} with {}", query, variables)

                val ctx = GraphQLContext(db, log, variables)
                val graphQL = GraphQL.newGraphQL(schema).build() // queryExecutionStrategy()
                val execution = ExecutionInput.Builder()
                        .query(query).variables(variables).context(ctx).root(ctx) // todo proper mutation root
                params.get("operationName")?.let { execution.operationName(it.toString()) }
                val executionResult = graphQL.execute(execution.build())

                result["data"] = executionResult.getData<Any?>()
                if (ctx.backLog.isNotEmpty()) {
                    result["extensions"] = ctx.backLog
                }
                if (executionResult.errors.isNotEmpty()) {
                    log.warn("Errors: {}", executionResult.errors)
                    result.put("errors", executionResult.errors)
                    tx.failure()
                } else {
                    tx.success()
                }
            } catch (e: Exception) {
                log.warn("Error executing " + params, e)
                val msg = e.message
                result.compute("errors") { k, v ->
                    when (v) { is List<*> -> v + msg; null -> listOf(msg); else -> listOf(v, msg)
                    }
                }
            }
            return result
        } finally {
            tx.close()
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun getVariables(requestBody: Map<String, Any?>): Map<String, Any> {
        val varParam = requestBody["variables"]
        return when (varParam) {
            is String -> parseMap(varParam)
            is Map<*, *> -> varParam as Map<String, Any>
            else -> emptyMap()
        }
    }

    private fun formatMap(result: Map<String, Any?>) = OBJECT_MAPPER.writeValueAsString(result)

    @Suppress("UNCHECKED_CAST")
    private fun parseMap(value: String?): Map<String, Any> =
            if (value == null || value.isNullOrBlank() || value == "null") emptyMap()
            else {
                val v = value.trim('"', ' ', '\t', '\n', '\r')
                OBJECT_MAPPER.readValue(v, Map::class.java) as Map<String, Any>
            }
}
