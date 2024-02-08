package yellowdog.platform

import co.yellowdog.platform.clients.scheduler.WorkClient
import co.yellowdog.platform.model.Task
import co.yellowdog.platform.model.TaskStatus
import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import groovy.transform.VisibilityOptions
import groovy.transform.options.Visibility
import nextflow.exception.AbortOperationException
import nextflow.processor.TaskConfig
import nextflow.processor.TaskId
import yellowdog.Constants

import java.time.Duration
import java.util.function.Supplier

@VisibilityOptions(constructor = Visibility.PRIVATE)
@TupleConstructor(includeFields = true)
@CompileStatic
class YdTask {

    private final String taskId

    private final WorkClient workClient

    private Supplier<Task> taskSupplier

    String getId() {
        taskId
    }

    TaskStatus getStatus() {
        taskSupplier.get().status
    }

    int getExitCode() {
        status == TaskStatus.COMPLETED ? 0 : Integer.MAX_VALUE
    }

    void abort() {
        workClient.cancelTask(taskId, true)
    }

    static YdTask addTask(
            TaskId taskId,
            TaskConfig taskConfig,
            String taskScript,
            String taskGroupId,
            boolean uploadTaskProcessOutput,
            WorkClient workClient
    ) {
        Task task = buildTask(taskId, taskConfig, taskScript, uploadTaskProcessOutput)
        String ydTaskId = addTaskToTaskGroup(task, taskGroupId, workClient)
        new YdTask(
                ydTaskId,
                workClient,
                new SingleTaskCachingFetcher(ydTaskId, workClient)::getTask
        )
    }

    private static Task buildTask(TaskId taskId, TaskConfig taskConfig, String taskScript, boolean uploadTaskProcessOutput) {
        try {
            Task.Builder builder = Task.builder()
                    .taskType(Constants.YD_TASK_TYPE)
                    .name("t" + taskId.toString())
                    .timeout(getTimeout(taskConfig))
                    .taskData(taskScript)
            if (uploadTaskProcessOutput) {
                builder.outputFromTaskProcess()
            }
            builder.build()
        } catch (Exception ex) {
            throw new AbortOperationException(String.format("Unable to build YellowDog task for Nextflow task (id: %s) -> %s", taskId, ex.message), ex)
        }
    }

    private static Duration getTimeout(TaskConfig taskConfig) {
        DirectiveMappingUtils.getMappedValue(
                taskConfig.getTime(),
                DirectiveMappingUtils.&getDuration,
                "time"
        )
    }

    private static String addTaskToTaskGroup(
            Task task,
            String taskGroupId,
            WorkClient workClient
    ) {
        try {
            workClient.addTasksToTaskGroup(
                    taskGroupId,
                    [task]
            ).first().id
        } catch (Exception ex) {
            throw new AbortOperationException(String.format("Unable to add task to task group (id: %s) -> %s", taskGroupId, ex.message), ex)
        }
    }
}
