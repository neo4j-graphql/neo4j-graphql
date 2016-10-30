package org.neo4j.graphql

class RelationshipInfo(val type: String, val label: String, val out: Boolean = true) {

    var multi: Boolean = false

    fun update(multi: Boolean): RelationshipInfo {
        this.multi = this.multi or multi
        return this
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false

        val that = other as RelationshipInfo

        return out == that.out && label == that.label && type == that.type
    }

    override fun hashCode(): Int {
        var result = label.hashCode()
        result = 31 * result + type.hashCode()
        result = 31 * result + if (out) 1 else 0
        return result
    }

    override fun toString(): String {
        return "RelationshipInfo{label='$label', type='$type', multi=$multi, out=$out}"
    }
}
