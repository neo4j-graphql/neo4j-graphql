package org.neo4j.graphql

import java.io.PrintWriter
import java.io.StringWriter

fun Throwable.stackTraceAsString() : String {
    val sw = StringWriter()
    this.printStackTrace(PrintWriter(sw))
    return sw.toString()
}
