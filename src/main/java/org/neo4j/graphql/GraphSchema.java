package org.neo4j.graphql;

import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import org.neo4j.graphdb.GraphDatabaseService;

/**
 * @author mh
 * @since 29.10.16
 */
public class GraphSchema {
    private static GraphQL graphql;

    public static GraphQL getGraphQL(GraphDatabaseService db) {
        if (graphql == null) {
            GraphQLSchema graphQLSchema = MetaData.buildSchema(db);
            graphql = new GraphQL(graphQLSchema);
        }
        return graphql;
    }
}
