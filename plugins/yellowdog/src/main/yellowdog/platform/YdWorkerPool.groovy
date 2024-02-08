package yellowdog.platform

import co.yellowdog.platform.clients.scheduler.WorkerPoolClient
import co.yellowdog.platform.interaction.compute.ComputeRequirementTemplateUsage
import co.yellowdog.platform.model.AutoShutdown
import co.yellowdog.platform.model.NodeWorkerTarget
import co.yellowdog.platform.model.ProvisionedWorkerPoolProperties
import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import groovy.transform.VisibilityOptions
import groovy.transform.options.Visibility
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException
import yellowdog.YellowDogExecutor
import yellowdog.config.WorkerPoolConfig

import java.time.Duration

@Slf4j
@CompileStatic
@VisibilityOptions(constructor = Visibility.PRIVATE)
@TupleConstructor(includeFields = true)
class YdWorkerPool {

    private static final String USER_DATA_RESOURCE_NAME = "/yellowdog/userdata.txt"

    private final String workerPoolId

    private final WorkerPoolClient workerPoolClient

    void shutdown() {
        workerPoolClient.shutdownWorkerPool(workerPoolId)
    }

    static YdWorkerPool addWorkerPool(
            String name,
            String namespace,
            WorkerPoolConfig config,
            WorkerPoolClient workerPoolClient
    ) {
        ComputeRequirementTemplateUsage requirementTemplateUsage
        ProvisionedWorkerPoolProperties provisionedProperties
        try {
            requirementTemplateUsage = buildRequirementTemplateUsage(name, namespace, config)
            provisionedProperties = buildProvisionedProperties(config)
        } catch (Exception ex) {
            throw new AbortOperationException("Unable to build worker pool request -> " + ex.message, ex)
        }
        String workerPoolId = submitWorkerPool(workerPoolClient, requirementTemplateUsage, provisionedProperties)
        new YdWorkerPool(workerPoolId, workerPoolClient)
    }

    private static String submitWorkerPool(
            WorkerPoolClient workerPoolClient,
            ComputeRequirementTemplateUsage requirementTemplateUsage,
            ProvisionedWorkerPoolProperties provisionedProperties
    ) {
        try {
            workerPoolClient.provisionWorkerPool(requirementTemplateUsage, provisionedProperties).id
        } catch (Exception ex) {
            throw new AbortOperationException("Unable to add worker pool -> " + ex.message, ex)
        }
    }

    private static ComputeRequirementTemplateUsage buildRequirementTemplateUsage(
            String name,
            String namespace,
            WorkerPoolConfig config
    ) {
        ComputeRequirementTemplateUsage.builder()
                .requirementName(name)
                .requirementNamespace(namespace)
                .templateId(config.getTemplateId())
                .imagesId(config.getImagesId())
                .targetInstanceCount(config.getStartNodes() ?: 0)
                .userData(getUserData())
                .build()
    }

    private static String getUserData() {
        URI uri = YellowDogExecutor.class.getResource(USER_DATA_RESOURCE_NAME).toURI()
        File file = new File(uri)
        file.text
    }

    private static ProvisionedWorkerPoolProperties buildProvisionedProperties(WorkerPoolConfig config) {
        ProvisionedWorkerPoolProperties.builder()
                .createNodeWorkers(buildNodeWorkersTarget(config))
                .minNodes(config.getMinNodes())
                .maxNodes(config.getMaxNodes())
                .workerTag(config.getWorkerTag())
                .idlePoolShutdown(buildAutoShutdown(config.idlePoolTimeout))
                .idleNodeShutdown(buildAutoShutdown(config.idleNodeTimeout))
                .nodeBootTimeout(config.nodeBootTimeout ? Duration.ofMillis(config.nodeBootTimeout.toMillis()) : null)
                .build()
    }

    private static NodeWorkerTarget buildNodeWorkersTarget(WorkerPoolConfig config) {

        if (config.workersPerNode) {
            if (config.workersPerVCPU) {
                log.warn("Both workersPerNode and workersPerVCPU configured for worker pool. workersPerNode value will be used.")
            }
            return NodeWorkerTarget.builder()
                    .perNode(config.workersPerNode)
                    .build()
        }

        if (config.workersPerVCPU) {
            return NodeWorkerTarget.builder()
                    .perVcpu(config.workersPerVCPU)
                    .build()
        }
    }

    private static AutoShutdown buildAutoShutdown(nextflow.util.Duration timeout) {
        if (timeout == null) return null
        long millis = timeout.toMillis()
        AutoShutdown.Builder builder = AutoShutdown.builder()
        if (millis > 0) {
            builder.enabled(true).timeout(Duration.ofMillis(millis))
        } else {
            builder.enabled(false)
        }
        builder.build()
    }
}
