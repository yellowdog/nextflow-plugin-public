package yellowdog.config


import groovy.transform.CompileStatic
import nextflow.exception.AbortOperationException

import static yellowdog.config.ConfigUtils.getOrDefaultString

@CompileStatic
class YellowDogConfig {

    static final String DEFAULT_PLATFORM_URL = "https://portal.yellowdog.co/api"

    static final String DEFAULT_NAMESPACE = "nextflow"

    private String platformUrl

    private ApiKeyConfig apiKey

    private String namespace

    private WorkRequirementConfig workRequirement

    private WorkerPoolConfig workerPool

    YellowDogConfig(Map map) {

        if (map == null) {
            throw new AbortOperationException("YellowDog Plugin configuration is missing")
        }

        platformUrl = getOrDefaultString(map, "platformUrl", DEFAULT_PLATFORM_URL)
        namespace = getOrDefaultString(map, "namespace", DEFAULT_NAMESPACE)
        apiKey = new ApiKeyConfig(map.apiKey as Map)
        if (map.workRequirement)
            workRequirement = new WorkRequirementConfig(map.workRequirement as Map)
        if (map.workerPool)
            workerPool = new WorkerPoolConfig(map.workerPool as Map)
    }

    String getPlatformUrl() { platformUrl }

    ApiKeyConfig getApiKey() { apiKey }

    String getNamespace() { namespace }

    WorkRequirementConfig getWorkRequirement() { workRequirement }

    WorkerPoolConfig getWorkerPool() { workerPool }
}
