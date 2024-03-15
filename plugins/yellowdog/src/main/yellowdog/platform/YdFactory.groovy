package yellowdog.platform

import co.yellowdog.platform.account.authentication.ApiKey
import co.yellowdog.platform.client.PlatformClient
import co.yellowdog.platform.model.ServicesSchema
import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import groovy.transform.VisibilityOptions
import groovy.transform.options.Visibility
import groovy.util.logging.Slf4j
import yellowdog.config.WorkRequirementConfig
import yellowdog.config.WorkerPoolConfig

@Slf4j
@CompileStatic
@VisibilityOptions(constructor = Visibility.PRIVATE)
@TupleConstructor(includeFields = true)
class YdFactory {

    private final PlatformClient ydClient

    private final String namespace

    void shutdown() {
        ydClient?.close()
    }

    YdWorkRequirement addWorkRequirement(
            String name,
            WorkRequirementConfig config
    ) {
        YdWorkRequirement.addWorkRequirement(
                name,
                namespace,
                config,
                ydClient.workClient()
        )
    }

    YdWorkerPool addWorkerPool(
            String name,
            WorkerPoolConfig config
    ) {
        YdWorkerPool.addWorkerPool(
                name,
                namespace,
                config,
                ydClient.workerPoolClient()
        )
    }

    static YdFactory create(
            String platformUrl,
            String apiKeyId,
            String apiKeySecret,
            String namespace
    ) {
        PlatformClient ydClient = PlatformClient.create(
                ServicesSchema.defaultSchema(platformUrl),
                new ApiKey(apiKeyId, apiKeySecret)
        )

        log.info("Platform Client created: {}", platformUrl)

        new YdFactory(ydClient, namespace)
    }
}
