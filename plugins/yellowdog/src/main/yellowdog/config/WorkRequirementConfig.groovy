package yellowdog.config

import groovy.transform.CompileStatic

import static yellowdog.config.ConfigUtils.getBoolean
import static yellowdog.config.ConfigUtils.getDouble

@CompileStatic
class WorkRequirementConfig {

    private Double priority

    private Boolean uploadTaskProcessOutput

    WorkRequirementConfig(Map config) {
        priority = getDouble(config, "priority")
        uploadTaskProcessOutput = getBoolean(config, "uploadTaskProcessOutput")
    }

    Double getPriority() { priority }

    Boolean uploadTaskProcessOutput() { uploadTaskProcessOutput }
}
