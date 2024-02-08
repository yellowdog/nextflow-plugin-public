package yellowdog.platform

import co.yellowdog.platform.clients.scheduler.WorkClient
import co.yellowdog.platform.model.RunSpecification
import co.yellowdog.platform.model.TaskGroup
import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import groovy.transform.VisibilityOptions
import groovy.transform.options.Visibility
import nextflow.processor.TaskConfig
import nextflow.processor.TaskId

@VisibilityOptions(constructor = Visibility.PRIVATE)
@TupleConstructor(includeFields = true)
@CompileStatic
class YdTaskGroup {

    private final String taskGroupId

    private final RunSpecification runSpecification

    private final boolean uploadTaskProcessOutput

    private final WorkClient workClient

    RunSpecification getRunSpecification() { runSpecification }

    YdTask addTask(TaskId taskId, TaskConfig taskConfig, String taskScript) {
        YdTask.addTask(
                taskId,
                taskConfig,
                taskScript,
                taskGroupId,
                uploadTaskProcessOutput,
                workClient
        )
    }

    static YdTaskGroup create(TaskGroup taskGroup, boolean uploadTaskProcessOutput, WorkClient workClient) {
        new YdTaskGroup(
                taskGroup.id,
                taskGroup.runSpecification,
                uploadTaskProcessOutput,
                workClient
        )
    }
}
