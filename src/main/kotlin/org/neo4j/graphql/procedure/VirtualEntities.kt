package org.neo4j.graphql.procedure

import org.neo4j.graphdb.*
import org.neo4j.graphql.joinNonEmpty
import java.util.concurrent.atomic.AtomicLong

open class VirtualEntity(val myId: Long, val props: MutableMap<String,Any>) : Entity {
    override fun getId() = myId

    override fun getAllProperties() = props

    override fun hasProperty(p0: String?): Boolean = props.containsKey(p0)

    override fun getGraphDatabase(): GraphDatabaseService {
        TODO("not implemented")
    }

    override fun setProperty(p0: String?, p1: Any?) {
        if (p0!=null && p1!=null) props.put(p0,p1)
    }
    
    override fun removeProperty(p0: String?) = props.remove(p0)

    override fun getProperties(vararg p0: String?) = p0.associate { it to props.get(it) }.toMutableMap()

    override fun getProperty(p0: String?) = props[p0]

    override fun getProperty(p0: String?, p1: Any?) = props.get(p0) ?: p1

    override fun getPropertyKeys() = props.keys
    fun delete() {
        TODO("not implemented")
    }

    override fun toString() = "<$myId> $props"
}
class VirtualRelationship(val start: Node, val type: String, props: Map<String,Any>, val end: Node) : VirtualEntity(maxId.decrementAndGet(), props.toMutableMap()), Relationship {
    companion object {
        val maxId = AtomicLong()
    }

    override fun getStartNode() = start
    override fun getEndNode() = end
    override fun getType() = RelationshipType { type }
    override fun getNodes() = arrayOf(start, end)
    override fun getOtherNode(p0: Node?) = if (start == p0) end else start
    override fun isType(p0: RelationshipType?) = p0?.name() == type
    override fun toString()= "${start}-[:$type ${super.toString()}]->${end} "
}
class VirtualNode(labels: List<String>, props: Map<String,Any>) : VirtualEntity(maxId.decrementAndGet(), props.toMutableMap()), Node {
    val rels = mutableListOf<Relationship>()
    val labels = labels.toMutableList()

    companion object {
        val maxId = AtomicLong()
    }

    override fun toString() = "${labels.joinNonEmpty(":",":")} ${super.toString()}"

    override fun getLabels(): MutableIterable<Label> = labels.map { Label.label(it) }.toMutableList()

    override fun addLabel(label: Label?) {
        labels.add(label!!.name())
    }

    override fun hasLabel(label: Label?) = labels.contains(label?.name())

    override fun getDegree() = rels.size

    override fun getDegree(type: RelationshipType?) = rels.filter { it.isType(type) }.size

    override fun getDegree(direction: Direction?) = rels.filter { hasDirection(direction, it) }.size

    private fun hasDirection(direction: Direction?, it: Relationship) = when (direction) {
        Direction.OUTGOING -> it.startNode == this
        Direction.INCOMING -> it.endNode == this
        else -> true
    }

    override fun getDegree(type: RelationshipType?, direction: Direction?) = rels.filter { it.isType(type) && hasDirection(direction, it) }.size

    override fun getRelationships(): MutableIterable<Relationship> = rels

    override fun getRelationships(vararg types: RelationshipType) = rels.filter { rel -> types.any { rel.isType(it) } }.toMutableList()

    override fun getRelationships(direction: Direction, vararg types: RelationshipType) =  rels.filter { rel -> hasDirection(direction, rel) && types.any { rel.isType(it) } }.toMutableList()

    override fun getRelationships(dir: Direction) = rels.filter { rel -> hasDirection(dir, rel) }.toMutableList()

    override fun getRelationships(type: RelationshipType, dir: Direction) =  rels.filter { rel -> hasDirection(dir, rel) && rel.isType(type) }.toMutableList()

    override fun removeLabel(label: Label?) {
        labels.remove(label?.name())
    }

    override fun getSingleRelationship(type: RelationshipType, dir: Direction) = getRelationships(type,dir).firstOrNull()

    override fun getRelationshipTypes(): MutableIterable<RelationshipType> = rels.associate { it.type.name() to it.type }.values.toMutableList()

    override fun createRelationshipTo(otherNode: Node, type: RelationshipType) = VirtualRelationship(this,type.name(), mutableMapOf(), otherNode)

    override fun hasRelationship() = rels.isNotEmpty()

    override fun hasRelationship(vararg types: RelationshipType) = getRelationships(*types).isNotEmpty()

    override fun hasRelationship(direction: Direction, vararg types: RelationshipType) = getRelationships(direction,*types).isNotEmpty()

    override fun hasRelationship(dir: Direction) = getDegree(dir) > 0

    override fun hasRelationship(type: RelationshipType, dir: Direction) = getDegree(type,dir) > 0
}
