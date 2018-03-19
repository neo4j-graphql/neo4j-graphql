package org.neo4j.graphql

import java.util.*

/**
 * @author mh
 * @since 30.10.16
 */
class MetaData(label: String) {
    var type = ""
    var isInterface = false
    var description : String? = null

    init {
        this.type = label
    }

    val properties = LinkedHashMap<String, PropertyInfo>()
    @JvmField val relationships: MutableMap<String, RelationshipInfo> = LinkedHashMap()
    val labels = LinkedHashSet<String>()

    override fun toString(): String {
        return "MetaData{type='$type', properties=$properties, labels=$labels, relationships=$relationships, isInterface=$isInterface}"
    }

    fun addIndexedProperty(name: String) {
        properties.compute(name, { name, prop -> prop?.copy(indexed = true) ?: PropertyInfo(name, PropertyType("String"),indexed = true) })
    }

    fun addIdProperty(name: String) {
        properties.compute(name, { name, prop -> prop?.copy(id = true) ?: PropertyInfo(name, PropertyType("String"),id = true) })
    }

    fun addLabel(label: String) {
        if (label != this.type) labels.add(label)
    }

    fun addProperty(name: String, javaClass: Class<Any>) {
        properties.compute(name, {name, prop -> prop?.copy(type = PropertyType(javaClass)) ?: PropertyInfo(name,PropertyType(javaClass)) })
    }

    fun addProperty(name: String, type: PropertyType, defaultValue: Any? = null, unique : Boolean = false, enum : Boolean = false, description: String? = null) {
        properties.compute(name, {name, prop -> (prop ?: PropertyInfo(name,type)).copy(type = type, defaultValue = defaultValue, unique = unique, enum = enum, description = description)})
    }

    fun addCypher(name: String, statement: String) {
        val cypherInfo = CypherInfo(statement)
        properties.computeIfPresent(name, { name, prop -> prop.copy(cypher = cypherInfo)})
        relationships.computeIfPresent(name, { name, rel -> rel.copy(cypher = cypherInfo)})
    }

    fun mergeRelationship(typeName:String, fieldName:String, label:String, out:Boolean, multi : Boolean, description: String?, nonNull:Int = 0) : RelationshipInfo {
        // fix for up
        val name = if (properties.containsKey(fieldName)) "_" + fieldName else fieldName
//        val name = if (out) "${typeName}_$label" else "${label}_${typeName}"
        return relationships.compute(name) { name,rel -> rel?.copy(multi = multi, out = out, description = description, nonNull = nonNull) ?: RelationshipInfo(name, typeName, label, out, multi, description = description, nonNull = nonNull) }!!
    }

    fun relationshipFor(fieldName: String) = relationships[fieldName]
    fun hasRelationship(fieldName: String) = relationships.containsKey(fieldName)

    fun cypherFor(fieldName: String) = relationships[fieldName]?.cypher?.cypher ?: properties[fieldName]?.cypher?.cypher

    data class PropertyType(val name: String, val array: Boolean = false, val nonNull: Int = 0, val enum: Boolean = false, val inputType: Boolean = false, val scalar: Boolean = false) {
        fun isBasic() : Boolean = basicTypes.contains(name) || scalar

        override fun toString(): String = (if (array) "[$name${(if (nonNull>1) "!" else "")}]" else name) + (if (nonNull>0) "!" else "")

        companion object {
            val basicTypes = setOf("String","Boolean","Float","Int","Number","ID")

            fun typeName(type: Class<*>): String {
                if (type.isArray) return typeName(type.componentType)
                if (type == String::class.java) return "String"
                if (type == Boolean::class.java || type == Boolean::class.javaObjectType) return "Boolean"
                if (Number::class.java.isAssignableFrom(type) || type.isPrimitive) {
                    if (type == Double::class.java || type == Double::class.javaObjectType || type == Float::class.java || type == Float::class.javaObjectType) return "Float"
                    return "Long"
                }
                throw IllegalArgumentException("Invalid type " + type.name)
            }
        }
        constructor(type: Class<*>) : this(typeName(type), type.isArray)
    }

    fun  isComputed(key: String) = properties[key]?.cypher != null
    /*
            if (type.isArray) {
            return GraphQLList(graphQlInType(type.componentType))
        }

     */
    data class ParameterInfo(val name: String, val type: PropertyType, val defaultValue: Any? = null, val description: String? = null) // todo directives
    data class CypherInfo(val cypher: String, val description: String? = null)
    data class PropertyInfo(val fieldName:String, val type: PropertyType, val id: Boolean = false,
                            val indexed: Boolean = false, val cypher: CypherInfo? = null, val defaultValue : Any? = null,
                            val unique: Boolean = false,val enum : Boolean = false,
                            val parameters : Map<String,ParameterInfo>? = null, val description : String? = null) {
        fun isGraphQLId() = type.name == "ID"
        fun isIdProperty() = isGraphQLId() || id
        fun isComputed() = cypher != null
        fun  updateable() = !isComputed() && !isIdProperty()
    }
    data class RelationshipInfo(val fieldName: String, val type: String, val label: String, val out: Boolean = true,
                                val multi: Boolean = false, val cypher: MetaData.CypherInfo? = null,
                                val parameters : Map<String,ParameterInfo>? = null,val description : String? = null,
                                val nonNull: Int = 0
    )

    fun addParameters(name: String, parameters: Map<String,ParameterInfo>) {
        if (parameters.isNotEmpty()) {
            properties.computeIfPresent(name, { name, prop -> prop.copy(parameters = parameters) })
            relationships.computeIfPresent(name, { name, rel -> rel.copy(parameters = parameters) })
        }
    }

    fun isInterface() {
        isInterface = true
    }

    fun  idProperty(): MetaData.PropertyInfo? = properties.values.firstOrNull { it.isGraphQLId() } ?: properties.values.firstOrNull { it.isIdProperty() }
}
