package org.neo4j.graphql

import org.neo4j.configuration.Config
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.availability.AvailabilityGuard
import org.neo4j.kernel.extension.ExtensionFactory
import org.neo4j.kernel.extension.ExtensionType
import org.neo4j.kernel.extension.context.ExtensionContext
import org.neo4j.kernel.lifecycle.Lifecycle
import org.neo4j.logging.internal.LogService
import org.neo4j.procedure.impl.GlobalProceduresRegistry

class GraphQLKernelExtensionFactory : ExtensionFactory<GraphQLKernelExtensionFactory.Dependencies>(ExtensionType.GLOBAL, "GraphQLExtension") {
    interface Dependencies {
        fun log(): LogService?
        fun config(): Config?
        fun globalProceduresRegistry(): GlobalProceduresRegistry?
        fun databaseManagementService(): DatabaseManagementService?
        fun availabilityGuard(): AvailabilityGuard?
    }

    override fun newInstance(context: ExtensionContext, dependencies: Dependencies): Lifecycle {
        return GraphQLExtension(dependencies.config(), dependencies.log(), dependencies.globalProceduresRegistry(), dependencies.databaseManagementService(), dependencies.availabilityGuard())
    }
}

class GraphQLExtension(config: Config?, log: LogService?, globalProceduresRegistry: GlobalProceduresRegistry?, databaseManagementService: DatabaseManagementService?, availabilityGuard: AvailabilityGuard?) : Lifecycle {
    init {
        globalProceduresRegistry!!.registerComponent(DatabaseManagementService::class.java, { databaseManagementService }, true)
        Thread {
            if (availabilityGuard!!.isAvailable(100000)) {
                try {
                    SchemaStorage.createConstraint(databaseManagementService!!)
                } catch (e: Exception) {
                    log!!.getUserLog(GraphQLKernelExtensionFactory::class.java)
                            .error("Error adding GraphQL constraint to system database", e)
                }
            } else {
                log!!.getUserLog(GraphQLKernelExtensionFactory::class.java).error("Error adding GraphQL constraint to system database, due to timout in startup")
            }
        }.start()
    }

    override fun shutdown() = Unit

    override fun start() = Unit

    override fun stop() = Unit

    override fun init() = Unit
}
