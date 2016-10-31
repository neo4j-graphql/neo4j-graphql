package org.neo4j.graphql

import org.neo4j.graphdb.Label
import java.util.*

/**
 * @author mh
 * @since 30.10.16
 */
class MetaData(label: Label) {

    var type = ""

    init {
        this.type = label.name()
    }

    private val ids = LinkedHashSet<String>()
    val indexed = LinkedHashSet<String>()
    val properties = LinkedHashMap<String, Class<*>>()
    val labels = LinkedHashSet<String>()
    @JvmField val relationships: MutableMap<String, RelationshipInfo> = LinkedHashMap()

    override fun toString(): String {
        return "MetaData{type='$type', ids=$ids, indexed=$indexed, properties=$properties, labels=$labels, relationships=$relationships}"
    }

    fun addIndexedProperty(property: String) {
        indexed.add(property)
    }

    fun addIdProperty(idProperty: String) {
        ids.add(idProperty)
    }

    fun addLabel(label: String) {
        if (label != this.type) labels.add(label)
    }

    fun addProperty(name: String, javaClass: Class<Any>) {
        properties.put(name,javaClass)
    }

    fun  relationshipFor(fieldName: String) = relationships[fieldName]
}
