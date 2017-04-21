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
        internal val allTypes = LinkedHashMap<String, MetaData>()

        val DENSE_NODE = 50

        @JvmStatic fun from(db: GraphDatabaseService, label: Label): MetaData {
            val metaData = MetaData(label.name())
            inspectIndexes(metaData, db, label)
            sampleNodes(metaData, db, label)
            return metaData
        }

        fun storeIdl(db: GraphDatabaseService, schema: String) : Map<String, MetaData> {
            val tx = db.beginTx()
            try {
                val metaDatas = IDLParser.parse(schema)
                graphProperties(db).setProperty("graphql.idl", schema)
                tx.success()
                return metaDatas
            } finally {
                tx.close()
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
                graphProperties(db).removeProperty("graphql.idl")
                tx.success()
            } finally {
                tx.close()
            }

        }
        fun readIdl(db: GraphDatabaseService) : Map<String, MetaData>? {
            val tx = db.beginTx()
            try {
                val schema = graphProperties(db).getProperty("graphql.idl", null) as String?
                val metaDatas = if (schema == null) null else IDLParser.parse(schema)
                tx.success()
                return metaDatas
            } finally {
                tx.close()
            }
        }

        fun databaseSchema(db: GraphDatabaseService) {
            if (allTypes.isEmpty()) {
                val idlMetaData = readIdl(db)
                if (idlMetaData != null) {
                    allTypes.putAll(idlMetaData)
                }
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

        private fun sampleNodes(md: MetaData, db: GraphDatabaseService, label: Label) {
            var count = 10
            val nodes = db.findNodes(label)
            val values = LinkedHashMap<String, Any>()
            while (nodes.hasNext() && count-- > 0) {
                val node = nodes.next()
                for (l in node.labels) md.addLabel(l.name())
                values.putAll(node.allProperties)
                sampleRelationships(md, node)
            }
            values.forEach { k, v -> md.addProperty(k, v.javaClass) }
        }


        private fun sampleRelationships(md: MetaData, node: Node) {
            val dense = node.degree > DENSE_NODE
            for (type in node.relationshipTypes) {
                val itOut = node.getRelationships(Direction.OUTGOING, type).iterator()
                val out = Iterators.firstOrNull(itOut)
                val typeName = type.name()
                if (out != null) {
                    if (!dense || node.getDegree(type, Direction.OUTGOING) < DENSE_NODE) {
                        labelsFor(out.endNode) { label -> md.mergeRelationship(typeName,typeName,label,true,itOut.hasNext()) }
                    }
                }
                val itIn = node.getRelationships(Direction.INCOMING, type).iterator()
                val `in` = Iterators.firstOrNull(itIn)
                if (`in` != null) {
                    if (!dense || node.getDegree(type, Direction.INCOMING) < DENSE_NODE) {
                        labelsFor(`in`.startNode) { label -> md.mergeRelationship(typeName,typeName,label,false,itIn.hasNext()) }
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
