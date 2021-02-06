package org.neo4j.graphql

import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.logging.internal.LogService
import org.neo4j.procedure.impl.GlobalProceduresRegistry

class GraphQLKernelExtensionFactory :
    ExtensionFactory<GraphQLKernelExtensionFactory.Dependencies>(ExtensionType.GLOBAL, "GraphQLExtension") {
    interface Dependencies {
        fun log(): LogService
        fun globalProceduresRegistry(): GlobalProceduresRegistry
        fun databaseManagementService(): DatabaseManagementService
    }

    override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
        return GraphQLExtension(
            dependencies.log(),
            dependencies.globalProceduresRegistry(),
            dependencies.databaseManagementService()
        )
    }
}

class GraphQLExtension(
    private val log: LogService,
    private val globalProceduresRegistry: GlobalProceduresRegistry,
    private val databaseManagementService: DatabaseManagementService
) : Lifecycle {

    override fun shutdown() = Unit

    override fun start() {
        try {
            SchemaStorage.createConstraint(databaseManagementService)
        } catch (e: Exception) {
            log.getUserLog(GraphQLKernelExtensionFactory::class.java)
                .error("Error adding GraphQL constraint to system database", e)
        }
    }

    override fun stop() = Unit

    override fun init() {
        globalProceduresRegistry.registerComponent(
            DatabaseManagementService::class.java,
            { databaseManagementService },
            true
        )
    }
}
