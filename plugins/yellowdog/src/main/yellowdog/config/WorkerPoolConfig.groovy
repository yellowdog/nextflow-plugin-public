package yellowdog.config

import groovy.transform.CompileStatic
import nextflow.util.Duration

import static yellowdog.config.ConfigUtils.*

@CompileStatic
class WorkerPoolConfig {

    private String templateId

    private String imagesId

    private Integer startNodes

    private Integer minNodes

    private Integer maxNodes

    private String workerTag

    private Integer workersPerNode

    private Double workersPerVCPU

    private Duration idlePoolTimeout

    private Duration idleNodeTimeout

    private Duration nodeBootTimeout

    WorkerPoolConfig(Map config) {
        templateId = requireString(config, "templateId")
        imagesId = getString(config, "imagesId")
        startNodes = getInteger(config, "startNodes")
        minNodes = getInteger(config, "minNodes")
        maxNodes = getInteger(config, "maxNodes")
        workerTag = getString(config, "workerTag")
        workersPerNode = getInteger(config, "workersPerNode")
        workersPerVCPU = getDouble(config, "workersPerVCPU")
        idlePoolTimeout = getDuration(config, "idlePoolTimeout")
        idleNodeTimeout = getDuration(config, "idleNodeTimeout")
        nodeBootTimeout = getDuration(config, "nodeBootTimeout")
    }

    String getTemplateId() { templateId }

    String getImagesId() { imagesId }

    Integer getStartNodes() { startNodes }

    Integer getMinNodes() { minNodes }

    Integer getMaxNodes() { maxNodes }

    String getWorkerTag() { workerTag }

    Integer getWorkersPerNode() { workersPerNode }

    Integer getWorkersPerVCPU() { workersPerVCPU }

    Duration getIdlePoolTimeout() { idlePoolTimeout }

    Duration getIdleNodeTimeout() { idleNodeTimeout }

    Duration getNodeBootTimeout() { nodeBootTimeout }
}
