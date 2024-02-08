package yellowdog.platform

import co.yellowdog.platform.model.CloudProvider
import co.yellowdog.platform.model.DoubleRange
import groovy.json.JsonDelegate
import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import groovy.transform.VisibilityOptions
import groovy.transform.options.Visibility
import nextflow.processor.TaskConfig

import java.time.Duration
import java.util.function.Function

import static yellowdog.platform.DirectiveMappingUtils.*

@VisibilityOptions(constructor = Visibility.PACKAGE_PRIVATE)
@TupleConstructor(includeFields = true)
@CompileStatic
class TaskGroupDirectiveMapper {

    private final TaskConfig taskConfig

    private Map extContent

    List<String> getInstanceTypes() {
        getMappedValue(
                taskConfig.machineType,
                DirectiveMappingUtils.&getStringList,
                "machineType"
        )
    }

    DoubleRange getVcpus() {
        getMappedValue(
                taskConfig.cpus,
                DirectiveMappingUtils.&getDoubleRangeMin,
                "cpus"
        )
    }

    DoubleRange getRam() {
        getMappedValue(
                taskConfig.memory,
                value -> getDoubleRangeMin(value, DirectiveMappingUtils.&parseMemoryToRam),
                "memory"
        )
    }

    Double getPriority() {
        getMappedExtValue("priority", DirectiveMappingUtils.&getDouble)
    }

    Duration getCompletedTaskTtl() {
        getMappedExtValue("completedTaskTtl", DirectiveMappingUtils.&getDuration)
    }

    List<CloudProvider> getProviders() {
        getMappedExtValue("provider", value -> getList(value, DirectiveMappingUtils.&parseProvider))
    }

    List<String> getRegions() {
        getMappedExtValue("region", DirectiveMappingUtils.&getStringList)
    }

    List<String> getWorkerTags() {
        getMappedExtValue("workerTag", DirectiveMappingUtils.&getStringList)
    }

    Integer getMinWorkers() {
        getMappedExtValue("minWorkers", DirectiveMappingUtils.&getInteger)
    }

    Integer getMaxWorkers() {
        getMappedExtValue("maxWorkers", DirectiveMappingUtils.&getInteger)
    }

    Integer getTasksPerWorker() {
        getMappedExtValue("tasksPerWorker", DirectiveMappingUtils.&getInteger)
    }

    Integer getMaximumTaskRetries() {
        getMappedExtValue("internalTaskRetries", DirectiveMappingUtils.&getInteger)
    }

    Boolean getExclusiveWorkers() {
        getMappedExtValue("exclusiveWorkers", DirectiveMappingUtils.&getBoolean)
    }

    private <T> T getMappedExtValue(String name, Function<Object, T> mapValue) {
        getMappedValue(
                ext[name],
                mapValue,
                "ext." + name
        )
    }

    private Map getExt() {
        if (extContent == null) {
            Object extValue = taskConfig.getRawValue("ext")
            if (extValue instanceof Closure) {
                extContent = JsonDelegate.cloneDelegateAndGetContent(extValue as Closure)
            } else if (extValue instanceof Map) {
                extContent = extValue as Map
            } else {
                extContent = Collections.emptyMap()
            }
        }
        extContent
    }
}
