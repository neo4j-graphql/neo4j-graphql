package org.neo4j.graphql

import java.io.PrintWriter
import java.io.StringWriter

fun Throwable.stackTraceAsString(): String {
    val sw = StringWriter()
    this.printStackTrace(PrintWriter(sw))
    return sw.toString()
}

fun <T> Iterable<T>.joinNonEmpty(separator: CharSequence = ", ", prefix: CharSequence = "", postfix: CharSequence = "", limit: Int = -1, truncated: CharSequence = "...", transform: ((T) -> CharSequence)? = null): String {
    return if (iterator().hasNext()) joinTo(StringBuilder(), separator, prefix, postfix, limit, truncated, transform).toString() else ""
}
