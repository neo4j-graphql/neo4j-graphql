package org.neo4j.graphql;

import graphql.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author mh
 * @since 29.10.16
 */
public class GraphQLProcedure {

    @Context
    public GraphDatabaseService db;

    @Procedure("graphql.execute")
    public Stream<GraphQLResult> execute(@Name("query") String query, @Name("variables") Map<String,Object> variables) {
        ExecutionResult result = GraphSchema.getGraphQL(db).execute(query, db, variables);

        if (result.getErrors().isEmpty()) return Stream.of(new GraphQLResult((Map)result.getData()));

        String errors = result.getErrors().stream().map(Object::toString).collect(Collectors.joining("\n"));
        throw new RuntimeException("Error executing GraphQL Query:\n"+errors);
    }
    public static class GraphQLResult {
        public final Map result;

        public GraphQLResult(Map result) {
            this.result = result;
        }
    }
}
