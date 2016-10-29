package org.neo4j.graphql;

import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.neo4j.helpers.collection.MapUtil.map;

/**
 * @author mh
 * @since 24.10.16
 */
@Path("/")
public class GraphQLResource {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static GraphQL graphql;
    private final Log log;
    private final GraphDatabaseService db;

    public GraphQLResource(@Context LogProvider provider, @Context GraphDatabaseService db) {
        this.db = db;
        this.log = provider.getLog(GraphQLResource.class);
        if (graphql == null) {
            GraphQLSchema graphQLSchema = MetaData.buildSchema(db);
            log.info(MetaData.getAllTypes().values().toString());
//            logSchema(log, graphQLSchema);
            graphql = new GraphQL(graphQLSchema);
        }
    }

    private void logSchema(Log log, GraphQLSchema graphQLSchema) {
        // todo provide / log as JSON
        log.bulk(l -> {
            l.info(graphQLSchema.getQueryType().toString());
            graphQLSchema.getAllTypesAsList().forEach(t -> l.info(t.toString()));
        });
    }


    @Path("/")
    @OPTIONS
    public Response options(@Context HttpHeaders headers) throws IOException {
        List<String> origins = headers.getRequestHeader("Origin");
        String origin = Iterables.firstOrNull(origins);
        Response.ResponseBuilder response = Response.ok();
        if (origin != null) {
            response = response.header("Access-Control-Allow-Origin", origin);
        }
        return response.build();
    }

    @Path("/")
    @GET
    public Response get(@QueryParam("query") String query, @QueryParam("variables") String variableParam) throws IOException {
        Map<String, Object> requestMap = map("query", query, "variables", variableParam);
        return executeQuery(requestMap);
    }

    @Path("/")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response executeOperation(String body) throws IOException {
        Map requestMap = OBJECT_MAPPER.readValue(body, Map.class);
        return executeQuery(requestMap);
    }

    private Response executeQuery(Map params) throws IOException {
        String query = (String) params.get("query");

        Map<String, Object> variables = getVariables(params);
        ExecutionResult executionResult = graphql.execute(query, db, variables);

        Map<String, Object> result = new LinkedHashMap<>();
        if (!executionResult.getErrors().isEmpty()) {
            result.put("errors", executionResult.getErrors());
            log.error("Errors: {}", executionResult.getErrors());
        }
        result.put("data", executionResult.getData());
        String responseString = OBJECT_MAPPER.writeValueAsString(result);
        return Response.ok().entity(responseString).build();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getVariables(Map requestBody) throws IOException {
        Object varParam = requestBody.get("variables");
        if (varParam instanceof String) return OBJECT_MAPPER.readValue((String) varParam, Map.class);
        if (varParam instanceof Map) return (Map<String, Object>) varParam;
        return Collections.emptyMap();
    }
}
