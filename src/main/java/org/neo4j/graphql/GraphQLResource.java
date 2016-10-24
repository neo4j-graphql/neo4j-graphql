package org.neo4j.graphql;

import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.*;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;

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
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response executeOperation(String body) throws IOException {
        Map requestBody = OBJECT_MAPPER.readValue(body,Map.class);
        String query = (String) requestBody.get("query");
        Map<String, Object> variables = (Map<String, Object>) requestBody.getOrDefault("variables",Collections.emptyMap());
        ExecutionResult executionResult = graphql.execute(query, db, variables);
        Map<String, Object> result = new LinkedHashMap<>();
        if (executionResult.getErrors().size() > 0) {
            result.put("errors", executionResult.getErrors());
            log.error("Errors: {}", executionResult.getErrors());
        }
        result.put("data", executionResult.getData());
        return Response.ok(OBJECT_MAPPER.writeValueAsString(result)).build();
    }
}
