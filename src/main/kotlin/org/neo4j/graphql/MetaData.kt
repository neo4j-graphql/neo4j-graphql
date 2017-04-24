package org.neo4j.graphql

import graphql.language.ListType
import graphql.language.NonNullType
import graphql.language.TypeName
import org.neo4j.graphdb.Label
import java.util.*

/**
 * @author mh
 * @since 30.10.16
 */
class MetaData(label: String) {

    var type = ""

    init {
        this.type = label
    }

    private val ids = LinkedHashSet<String>()
    val indexed = LinkedHashSet<String>()
    val properties = LinkedHashMap<String, PropertyType>()
    val labels = LinkedHashSet<String>()
    @JvmField val relationships: MutableMap<String, RelationshipInfo> = LinkedHashMap()
    val cypher = LinkedHashMap<String, String>()

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
        properties.put(name, PropertyType(javaClass))
    }

    fun addProperty(name: String, type: PropertyType) {
        properties.put(name, type)
    }

    fun addCypher(name: String, statement: String) {
        cypher.put(name, statement)
    }

    fun mergeRelationship(typeName:String, fieldName:String, label:String, out:Boolean = true, multi : Boolean = false) : RelationshipInfo {
//        val name = if (out) "${typeName}_$label" else "${label}_${typeName}"
        return relationships.getOrPut(fieldName) { RelationshipInfo(fieldName, typeName, label, out) }.update(multi)
    }

    fun relationshipFor(fieldName: String) = relationships[fieldName]

    fun cypherFor(fieldName: String) = cypher[fieldName]

    data class PropertyType(val name: String, val array: Boolean = false, val nonNull: Boolean = false) {
        fun isBasic() : Boolean = basicTypes.contains(name)

        companion object {
            val basicTypes = setOf("String","Boolean","Float","Int","Number","ID")

            fun typeName(type: Class<*>): String {
                if (type.isArray) return typeName(type.componentType)
                if (type == String::class.java) return "String"
                if (type == Boolean::class.java || type == Boolean::class.javaObjectType) return "Boolean"
                if (Number::class.java.isAssignableFrom(type) || type.isPrimitive) {
                    if (type == Double::class.java || type == Double::class.javaObjectType || type == Float::class.java || type == Float::class.javaObjectType) return "Float"
                    return "Int"
                }
                throw IllegalArgumentException("Invalid type " + type.name)
            }
        }
        constructor(type: Class<*>) : this(typeName(type), type.isArray)
    }
    /*
            if (type.isArray) {
            return GraphQLList(graphQlInType(type.componentType))
        }

     */
}
