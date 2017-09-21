FROM openjdk:8-jdk

# Install git and maven.
RUN apt-get update && apt-get install -y --no-install-recommends git maven

# Get the neo4j-graphql source.
RUN git clone https://github.com/neo4j-graphql/neo4j-graphql
WORKDIR neo4j-graphql
RUN git checkout 3.2

# Must create the target directory before building the package into it.
RUN mkdir /neo4j-graphql/target

# Build the package.
RUN mvn clean package
