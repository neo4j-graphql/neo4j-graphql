package org.neo4j.graphql;

import graphql.Scalars;
import graphql.language.*;
import graphql.schema.*;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.Iterators;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static graphql.Scalars.GraphQLString;
import static graphql.schema.GraphQLArgument.newArgument;
import static graphql.schema.GraphQLFieldDefinition.newFieldDefinition;
import static graphql.schema.GraphQLObjectType.newObject;

/**
 * @author mh
 * @since 24.10.16
 */
public class MetaData {
    public static final int DENSE_NODE = 50;
    private static Map<String, MetaData> allTypes = new LinkedHashMap<>();
    private String type;
    private Set<String> ids = new LinkedHashSet<>();
    private Set<String> indexed = new LinkedHashSet<>();
    private Map<String, Class<?>> properties = new LinkedHashMap<>();
    private Set<String> labels = new LinkedHashSet<>();
    Map<String, RelationshipInfo> relationships = new LinkedHashMap<>();

    static class RelationshipInfo {
        String label;
        String type;
        boolean multi;
        boolean out;

        static RelationshipInfo from(String type, String label, boolean multi, boolean out) {
            RelationshipInfo info = new RelationshipInfo();
            info.label = label;
            info.type = type;
            info.multi |= multi;
            info.out = out;
            return info;
        }

        RelationshipInfo update(boolean multi) {
            this.multi |= multi;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RelationshipInfo that = (RelationshipInfo) o;

            return out == that.out && label.equals(that.label) && type.equals(that.type);
        }

        @Override
        public int hashCode() {
            int result = label.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + (out ? 1 : 0);
            return result;
        }

        @Override
        public String toString() {
            return "RelationshipInfo{" +
                    "label='" + label + '\'' +
                    ", type='" + type + '\'' +
                    ", multi=" + multi +
                    ", out=" + out +
                    '}';
        }
    }
    public MetaData(GraphDatabaseService db, Label label) {
        this.type = label.name();
        inspectIndexes(db, label);
        sampleNodes(db, label);
    }

    @Override
    public String toString() {
        return "MetaData{" +
                "type='" + type + '\'' +
                ", ids=" + ids +
                ", indexed=" + indexed +
                ", properties=" + properties +
                ", labels=" + labels +
                ", relationships=" + relationships +
                '}';
    }

    public static GraphQLSchema buildSchema(GraphDatabaseService db) {
        databaseSchema(db);

        GraphQLSchema.Builder schema = new GraphQLSchema.Builder();

        GraphQLObjectType mutationType = null;
//        schema = schema.mutation(mutationType);

        GraphQLObjectType queryType = newObject().name("QueryType").fields(queryFields()).build();
        schema = schema.query(queryType);

        return schema.build(graphQlTypes());
    }

    private static void databaseSchema(GraphDatabaseService db) {
        allTypes.clear();
        try (Transaction tx = db.beginTx()) {
            for (Label label : db.getAllLabels()) {
                MetaData metaData = from(db, label);
                allTypes.put(label.name(), metaData);
            }
            tx.success();
        }
    }

    public static Map<String, MetaData> getAllTypes() {
        return allTypes;
    }

    private void inspectIndexes(GraphDatabaseService db, Label label) {
        for (IndexDefinition index : db.schema().getIndexes(label)) {
            for (String s : index.getPropertyKeys()) {
                if (index.isConstraintIndex()) ids.add(s);
                indexed.add(s);
            }
        }
    }

    public GraphQLInterfaceType toGraphQLInterface() {
        GraphQLInterfaceType.Builder builder = GraphQLInterfaceType.newInterface()
                .typeResolver(new TypeResolver() {
                    @Override
                    public GraphQLObjectType getType(Object object) {
                        return allTypes.get(object.toString()).toGraphQL();
                    }
                })
                .name(type)
                .description(type + "-Label");
        // it uses the interface types for checking against parent-types of fields, which is weird
        builder = addProperties(builder, ids, indexed); // mostly id, indexed, not sure about others
        return builder.build();
    }

    private GraphQLInterfaceType.Builder addProperties(GraphQLInterfaceType.Builder builder, Set<String> ids, Set<String> indexed) {
        for (String key : indexed) {
            builder = builder.field(newField(key, properties.get(key)));
        }
        return builder;
    }

    public GraphQLObjectType toGraphQL() {
        GraphQLObjectType.Builder builder = newObject()
                .name(type)
                .description(type + "-Node");

        builder = builder.field(newFieldDefinition()
                .name("_id")
                .description("internal node id")
//                    .fetchField().dataFetcher((env) -> null)
                .type(Scalars.GraphQLID).build());


// todo relationships, labels etc.

// something is off with rule-checking probably interface names conflicting with object names
//        builder = addInterfaces(builder);
        builder = addProperties(builder);
        builder = addRelationships(builder);
        return builder.build();
    }

    private GraphQLObjectType.Builder addRelationships(GraphQLObjectType.Builder builder) {
        for (Map.Entry<String, RelationshipInfo> entry : relationships.entrySet()) {
            RelationshipInfo info = entry.getValue();
            builder = builder.field(newReferenceField(entry.getKey(), info.label,info.multi));
        }
        return builder;
    }

    private GraphQLObjectType.Builder addProperties(GraphQLObjectType.Builder builder) {
        for (Map.Entry<String, Class<?>> entry : properties.entrySet()) {
            String name = entry.getKey();
            Class<?> type = entry.getValue();
            builder = builder.field(newField(name, type));
        }
        return builder;
    }

    private GraphQLFieldDefinition newReferenceField(String name, String label, boolean multi) {
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
                .description(this.type+" "+name+" "+label)
                .type(multi ? new GraphQLList(new GraphQLTypeReference(label)) : new GraphQLTypeReference(label))
                .build();
    }
    private GraphQLFieldDefinition newField(String name, Class<?> type) {
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
                .build();
    }

    private GraphQLObjectType.Builder addInterfaces(GraphQLObjectType.Builder builder) {
        for (String label : labels) {
            builder = builder.withInterface(allTypes.get(label).toGraphQLInterface());
        }
        return builder;
    }

    public static MetaData from(GraphDatabaseService db, Label label) {
        return new MetaData(db, label);
    }

    private void sampleNodes(GraphDatabaseService db, Label label) {
        int count = 10;
        ResourceIterator<Node> nodes = db.findNodes(label);
        Map<String, Object> values = new LinkedHashMap<>();
        while (nodes.hasNext() && count-- > 0) {
            Node node = nodes.next();
            for (Label l : node.getLabels()) labels.add(l.name());
            values.putAll(node.getAllProperties());
            sampleRelationships(node);
        }
        values.forEach((k, v) -> properties.put(k, v.getClass()));
        labels.remove(type);
    }

    private void sampleRelationships(Node node) {
        boolean dense = node.getDegree() > DENSE_NODE;
        for (RelationshipType type : node.getRelationshipTypes()) {
            Iterator<Relationship> itOut = node.getRelationships(Direction.OUTGOING, type).iterator();
            Relationship out = Iterators.firstOrNull(itOut);
            String typeName = type.name();
            if (out != null) {
                if (!dense || node.getDegree(type,Direction.OUTGOING)<DENSE_NODE) {
                    String outName = typeName + "_";
                    labelsFor(out.getEndNode(),(label)-> this.relationships.compute(outName+label, (key,info) ->
                            info == null ?
                                    RelationshipInfo.from(typeName,label,itOut.hasNext(), true) :
                                    info.update(itOut.hasNext())));
                }
            }
            Iterator<Relationship> itIn = node.getRelationships(Direction.INCOMING, type).iterator();
            Relationship in = Iterators.firstOrNull(itIn);
            if (in != null) {
                if (!dense || node.getDegree(type,Direction.INCOMING)<DENSE_NODE) {
                    String inName = "_"+typeName;
                    labelsFor(in.getStartNode(),(label)-> this.relationships.compute(label+inName,(key,info) ->
                            info == null ?
                                    RelationshipInfo.from(typeName,label,itIn.hasNext(), false) :
                                    info.update(itIn.hasNext())));
                }
            }
        }
    }


    private void labelsFor(Node node, Consumer<String> consumer) {
        for (Label label : node.getLabels()) {
            consumer.accept(label.name());
        }
    }

    private static GraphQLOutputType graphQlOutType(Class<?> type) {
        if (type.equals(String.class)) return GraphQLString;
        if (Number.class.isAssignableFrom(type)) {
            if (type.equals(Double.class) || type.equals(Float.class)) return Scalars.GraphQLFloat;
            return Scalars.GraphQLLong;
        }
        if (type.equals(Boolean.class)) return Scalars.GraphQLBoolean;
        if (type.getClass().isArray()) {
            return new GraphQLList(graphQlOutType(type.getComponentType()));
        }
        throw new IllegalArgumentException("Unknown field type "+type);
    }

    private static GraphQLInputType graphQlInType(Class<?> type) {
        if (type.equals(String.class)) return GraphQLString;
        if (Number.class.isAssignableFrom(type)) {
            if (type.equals(Double.class) || type.equals(Float.class)) return Scalars.GraphQLFloat;
            return Scalars.GraphQLLong;
        }
        if (type.equals(Boolean.class)) return Scalars.GraphQLBoolean;
        if (type.getClass().isArray()) {
            return new GraphQLList(graphQlInType(type.getComponentType()));
        }
        throw new IllegalArgumentException("Unknown field type "+type);
    }

    public static List<GraphQLFieldDefinition> queryFields() {
        return allTypes.values().stream()
                .map(md -> {
                    return newFieldDefinition()
                            .name(md.type)
                            .type(new GraphQLList(md.toGraphQL()))
                            .argument(md.propertiesAsArguments())
//                            .fetchField();
                            .dataFetcher((env) -> {
                                return fetchGraphData(md, env);
                            }).build();
                }).collect(Collectors.toList());
    }

    private static Object fetchGraphData(MetaData md, DataFetchingEnvironment env) {
        GraphDatabaseService db = (GraphDatabaseService) env.getContext();
        return env.getFields()
                .stream().map(MetaData::generateQueryForField)
                .flatMap((query) -> {
                    Map<String, Object> parameters = env.getArguments();
                    Result result = db.execute(query, parameters);
                    return Iterators.asList(result).stream(); // todo map of name -> data
                }).collect(Collectors.toList());
    }

    private static String generateQueryForField(Field field) {
        String name = field.getName();
        String variable = name;
        MetaData metaData = allTypes.get(name);

        String query = String.format("MATCH (%s:`%s`) \n",variable, name);
        query += addWhere(field, variable);
        for (Selection selection : field.getSelectionSet().getSelections()) {
            if (selection instanceof Field) {
                String fieldName = ((Field) selection).getName();
                RelationshipInfo info = metaData.relationships.get(fieldName);
                if (info != null) {
                    query += String.format(" OPTIONAL MATCH (`%s`)%s-[:`%s`]-%s(`%s`:`%s`) \n",variable, !info.out ? "<" : "", info.type, info.out ? ">" : "", fieldName, info.label);
// todo handle conditions on related elements after optional match
//TODO                    query += addWhere(field, variable);
                }
            }
        }
        query += " RETURN ";
        query += selectionFields(field, variable, metaData,false);
        System.out.println("query = " + query);
        return query;
    }

    private static String addWhere(Field field, String variable) {
        if (field.getArguments().isEmpty()) {
            return "";
        }
        String query = " WHERE ";
        Iterator<Argument> args = field.getArguments().iterator();
        while (args.hasNext()) {
            Argument argument = args.next();
            query += String.format("%s.`%s` = ", variable, argument.getName());
            Value value = argument.getValue();
            if (value instanceof VariableReference) query += String.format("{`%s`}", ((VariableReference) value).getName());
            // todo turn into parameters
            if (value instanceof IntValue) query += ((IntValue)value).getValue();
            if (value instanceof StringValue) query += "\""+((StringValue)value).getValue()+"\"";
            if (args.hasNext()) query += " AND \n";
        }
        return query;
    }

    private static String selectionFields(Field field, String variable, MetaData metaData, boolean prefix) {
        SelectionSet selectionSet = field.getSelectionSet();
        if (selectionSet==null) return "";

        Iterator<Selection> it = selectionSet.getSelections().iterator();
        String query = "";
        while (it.hasNext()) {
            Field f = (Field) it.next();
            String fieldName = f.getName();
            String alias = f.getAlias() != null ? f.getAlias() : fieldName;
            RelationshipInfo info = metaData.relationships.get(fieldName); // todo correct medatadata of
            if (prefix) {
                query += alias + ": ";
            }
            if (info == null) {
                query += String.format("`%s`.`%s`", variable, fieldName);
            } else {
                if (f.getSelectionSet() != null ) {
                    String subFields = selectionFields(f, f.getName(), allTypes.get(info.label), true);
                    query += String.format(info.multi ? "collect({%s})" : "{%s}", subFields);
                }
            }
            if (!prefix) {
                query += " AS " + alias;
            }
            if (it.hasNext()) query += ",\n";
        }
        return query;
    }

    private List<GraphQLArgument> propertiesAsArguments() {
        return properties.entrySet().stream().map(e ->
                newArgument()
                        .name(e.getKey())
                        .description(e.getKey()+" of "+type)
                        .type(graphQlInType(e.getValue())).build()
        ).collect(Collectors.toList());
    }
    public static Set<GraphQLType> graphQlTypes() {
        return allTypes.values().stream()
                .map(MetaData::toGraphQL)
                .collect(Collectors.toSet());
    }
}
