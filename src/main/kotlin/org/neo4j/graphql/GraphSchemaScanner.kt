package org.neo4j.graphql

import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Node
import org.neo4j.helpers.collection.Iterators
import java.util.*

class GraphSchemaScanner {
    companion object {
        internal val allTypes = LinkedHashMap<String, MetaData>()

        val DENSE_NODE = 50

        @JvmStatic fun from(db: GraphDatabaseService, label: Label): MetaData {
            val metaData = MetaData(label)
            inspectIndexes(metaData, db, label)
            sampleNodes(metaData, db, label)
            return metaData
        }

        fun databaseSchema(db: GraphDatabaseService) {
            allTypes.clear()

            val tx = db.beginTx()
            try {
                for (label in db.allLabels) {
                    allTypes.put(label.name(), from(db, label))
                }
                tx.success()
            } finally {
                tx.close()
            }
/*todo doesn't work like this
    db.beginTx().use { tx :Transaction ->
        tx.success()
    }
*/
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
                        val outName = typeName + "_"
                        labelsFor(out.endNode) { label ->
                            md.relationships.getOrPut(outName + label)
                            { RelationshipInfo(typeName, label, true) }
                                    .update(itOut.hasNext())
                        }
                    }
                }
                val itIn = node.getRelationships(Direction.INCOMING, type).iterator()
                val `in` = Iterators.firstOrNull(itIn)
                if (`in` != null) {
                    if (!dense || node.getDegree(type, Direction.INCOMING) < DENSE_NODE) {
                        val inName = "_" + typeName
                        labelsFor(`in`.startNode) { label ->
                            md.relationships.getOrPut(label + inName)
                            { RelationshipInfo(typeName, label, false) }
                                    .update(itIn.hasNext())
                        }
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
