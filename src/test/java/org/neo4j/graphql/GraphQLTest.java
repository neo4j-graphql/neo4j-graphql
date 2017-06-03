package org.neo4j.graphql;

import graphql.TypeResolutionEnvironment;
import graphql.schema.*;
import org.junit.Test;

import static graphql.Scalars.*;
import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLEnumType.newEnum;
import static graphql.schema.GraphQLFieldDefinition.*;
import static graphql.schema.GraphQLInputObjectField.newInputObjectField;
import static graphql.schema.GraphQLInputObjectType.newInputObject;
import static graphql.schema.GraphQLInterfaceType.newInterface;
import static graphql.schema.GraphQLObjectType.*;
import static graphql.schema.GraphQLUnionType.newUnionType;

/**
 * @author mh
 * @since 24.10.16
 */
public class GraphQLTest {
    @Test
    public void testCreateObjectType() throws Exception {
        GraphQLObjectType simpsonCharacter = newObject()
                .name("SimpsonCharacter")
                .description("A Simpson character")
                .field(newFieldDefinition()
                        .name("name")
                        .description("The name of the character.")
                        .type(GraphQLString).build())
                .field(newFieldDefinition()
                        .name("mainCharacter")
                        .description("One of the main Simpson characters?")
                        .type(GraphQLBoolean).build())
                .build();

    }

    @Test
    public void testCreateInterface() throws Exception {
        GraphQLObjectType CatType = newObject().name("CatType").build();

        GraphQLInterfaceType comicCharacter = newInterface()
                .name("ComicCharacter")
                .description("A abstract comic character.")
                .field(newFieldDefinition()
                        .name("name")
                        .description("The name of the character.")
                        .type(GraphQLString).build())
                .typeResolver((env) -> CatType)
                .build();
    }

    static class Dog {}
    static class Cat {}
    @Test
    public void testCreateUnionType() throws Exception {
        GraphQLObjectType CatType = newObject().name("CatType").build();
        GraphQLObjectType DogType = newObject().name("DogType").build();

        GraphQLUnionType PetType = newUnionType()
                .name("Pet")
                .possibleType(CatType)
                .possibleType(DogType)
                .typeResolver(new TypeResolver() {
                    @Override
                    public GraphQLObjectType getType(TypeResolutionEnvironment env) {
                        Object object = env.getObject();
                        if (object instanceof Cat) {
                            return CatType;
                        }
                        if (object instanceof Dog) {
                            return DogType;
                        }
                        return null;
                    }
                })
                .build();
    }

    @Test
    public void createEnum() throws Exception {
        GraphQLEnumType colorEnum = newEnum()
                .name("Color")
                .description("Supported colors.")
                .value("RED")
                .value("GREEN")
                .value("BLUE")
                .build();
    }

    @Test
    public void objectInput() throws Exception {
        GraphQLInputObjectType inputObjectType = newInputObject()
                .name("inputObjectType")
                .field(newInputObjectField()
                        .name("field")
                        .type(GraphQLString).build())
                .build();
    }

    @Test
    public void lists() throws Exception {
        new GraphQLList(GraphQLString); // a list of Strings

        new GraphQLNonNull(GraphQLString); // a non null String
    }

    @Test
    public void schema() throws Exception {
        GraphQLObjectType queryType = GraphQLObjectType.newObject().name("QueryType").build();
        GraphQLSchema schema = GraphQLSchema.newSchema()
                .query(queryType) // must be provided
//                .mutation(mutationType) // is optional
                .build();
    }

    @Test
    public void recursiveRelationships() throws Exception {
        GraphQLObjectType person = newObject()
                .name("Person")
                .field(newFieldDefinition()
                        .name("friends")
                        .type(new GraphQLList(new GraphQLTypeReference("Person"))).build())
                .build();

    }

/*
    @Test
    public void dataFetcher() throws Exception {
        DataFetcher calculateComplicatedValue = new DataFetcher() {
            @Override
            Object get(DataFetchingEnvironment environment) {
                // environment.getSource() is the value of the surrounding
                // object. In this case described by objectType
                Object value = null;// ... // Perhaps getting from a DB or whatever
                return value;
            }

            GraphQLObjectType objectType = newObject()
                    .name("ObjectType")
                    .field(newFieldDefinition()
                            .name("someComplicatedValue")
                            .type(GraphQLString)
                            .dataFetcher(calculateComplicatedValue))
                    .build();

        }
    }
*/
    /*
    @Test
    public void lambda() throws Exception {
        GraphQLObjectType queryType = newObject()
                .name("helloWorldQuery")
                .field(field -> field.(GraphQLString)
                        .name("hello")
                        .argument(argument -> argument.name("arg")
                                .type(GraphQLBoolean))
                        .dataFetcher(env -> "hello"))
                .build();
    }
    */
}
