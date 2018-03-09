package org.neo4j.graphql

import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.helpers.collection.Iterators
import org.neo4j.kernel.impl.core.GraphProperties
import org.neo4j.kernel.impl.core.NodeManager
import org.neo4j.kernel.internal.GraphDatabaseAPI
import java.util.*

class GraphSchemaScanner {
    companion object {
        fun fieldName(type: String) : String = type.split("_").mapIndexed { i, s -> if (i==0) s.toLowerCase() else s.toLowerCase().capitalize()  }.joinToString("")
        internal val allTypes = LinkedHashMap<String, MetaData>()
        internal var schema : String? = null

        val DENSE_NODE = 50

        @JvmStatic fun from(db: GraphDatabaseService, label: Label): MetaData {
            val metaData = MetaData(label.name())
            inspectIndexes(metaData, db, label)
            sampleNodes(metaData, db, label)
            return metaData
        }

        fun storeIdl(db: GraphDatabaseService, schema: String) : Map<String, MetaData> {
            val metaDatas = IDLParser.parse(schema)
            val tx = db.beginTx()
            try {
                graphProperties(db).setProperty("graphql.idl", schema)
                tx.success()
                return metaDatas
            } finally {
                tx.close()
                GraphSchema.reset()
            }
        }

        private fun graphProperties(db: GraphDatabaseService): GraphProperties {
            val nodeManager = (db as (GraphDatabaseAPI)).getDependencyResolver().resolveDependency(NodeManager::class.java)
            val props = nodeManager.newGraphProperties();
            return props
        }

        fun deleteIdl(db: GraphDatabaseService) {
            val tx = db.beginTx()
            try {
                val props = graphProperties(db)
                if (props.hasProperty("graphql.idl")) props.removeProperty("graphql.idl")
                tx.success()
            } finally {
                tx.close()
            }

        }
        fun readIdlMetadata(db: GraphDatabaseService) = readIdl(db)?.let { IDLParser.parse(it) }

        fun readIdl(db: GraphDatabaseService) : String? {
            val tx = db.beginTx()
            try {
                val schema = graphProperties(db).getProperty("graphql.idl", null) as String?
                tx.success()
                return schema
            } finally {
                tx.close()
            }
        }

        fun databaseSchema(db: GraphDatabaseService) {
            allTypes.clear();
            schema = readIdl(db)
            val idlMetaData = readIdlMetadata(db)
            if (idlMetaData != null) {
                allTypes.putAll(idlMetaData)
            }
            if (allTypes.isEmpty()) {
                allTypes.putAll(sampleDataBase(db))
            }
        }

        fun allTypes(): Map<String, MetaData> = allTypes
        fun allMetaDatas() = allTypes.values

        fun getMetaData(type: String): MetaData? {
            return allTypes[type]
        }

        private fun inspectIndexes(md: MetaData, db: GraphDatabaseService, label: Label) {
            for (index in db.schema().getIndexes(label)) {
                for (s in index.propertyKeys) {
                    if (index.isConstraintIndex) md.addIdProperty(s)
                    md.addIndexedProperty(s)
                }
            }
        }

        private fun sampleDataBase(db: GraphDatabaseService): Map<String, MetaData> {
            val tx = db.beginTx()
            try {
                val result = db.allLabels.associate { label -> label.name() to from(db,label) }
                tx.success()
                return result
            } finally {
                tx.close()
            }
        }

        private fun sampleNodes(md: MetaData, db: GraphDatabaseService, label: Label) {
            var count = 10
            val nodes = db.findNodes(label)
            while (nodes.hasNext() && count-- > 0) {
                val node = nodes.next()
                for (l in node.labels) md.addLabel(l.name())
                node.allProperties.forEach { k, v -> md.addProperty(k, v.javaClass) }
                sampleRelationships(md, node)
            }
        }


        private fun sampleRelationships(md: MetaData, node: Node) {
            val dense = node.degree > DENSE_NODE
            for (type in node.relationshipTypes) {
                val itOut = node.getRelationships(Direction.OUTGOING, type).iterator()
                val out = Iterators.firstOrNull(itOut)
                val typeName = type.name()
                val fieldName = fieldName(typeName) // todo handle end-label
                if (out != null) {
                    if (!dense || node.getDegree(type, Direction.OUTGOING) < DENSE_NODE) {
                        labelsFor(out.endNode) { label -> md.mergeRelationship(typeName, fieldName,label,true,itOut.hasNext(),null,0) }
                    }
                }
                val itIn = node.getRelationships(Direction.INCOMING, type).iterator()
                val `in` = Iterators.firstOrNull(itIn)
                if (`in` != null) {
                    if (!dense || node.getDegree(type, Direction.INCOMING) < DENSE_NODE) {
                        labelsFor(`in`.startNode) { label -> md.mergeRelationship(typeName, fieldName,label,false,itIn.hasNext(),null,0) }
                    }
                }
            }
        }

        private fun labelsFor(node: Node, consumer: (String) -> Unit) {
            for (label in node.labels) {
                consumer.invoke(label.name())
            }
        }
    }
}
