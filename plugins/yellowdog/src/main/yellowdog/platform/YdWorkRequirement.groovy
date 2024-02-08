package yellowdog.platform

import co.yellowdog.platform.clients.scheduler.WorkClient
import co.yellowdog.platform.interaction.pagination.SliceReference
import co.yellowdog.platform.interaction.scheduler.TaskSearch
import co.yellowdog.platform.model.RunSpecification
import co.yellowdog.platform.model.TaskGroup
import co.yellowdog.platform.model.TaskStatus
import co.yellowdog.platform.model.WorkRequirement
import co.yellowdog.platform.model.WorkRequirementStatus
import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import groovy.transform.VisibilityOptions
import groovy.transform.options.Visibility
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException
import nextflow.processor.TaskRun
import yellowdog.Constants
import yellowdog.config.WorkRequirementConfig

import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.function.Consumer

import static co.yellowdog.util.CollectionUtils.unmodifiableEnumSetOf
import static yellowdog.YellowDogUtils.sanitizeName

@Slf4j
@VisibilityOptions(constructor = Visibility.PRIVATE)
@TupleConstructor(includeFields = true)
@CompileStatic
class YdWorkRequirement {

    static final Duration DEFAULT_COMPLETED_TASK_TTL = Duration.ofMinutes(10)

    static final int DEFAULT_MAXIMUM_TASK_RETRIES = 0;

    private static final List<TaskStatus> UNFINISHED_TASK_STATUSES = unmodifiableEnumSetOf(TaskStatus, status -> !status.finished).toList()

    private final String workRequirementId

    private WorkRequirementConfig config

    private final WorkClient workClient

    private final Map<String, YdTaskGroup> processTaskGroups = new ConcurrentHashMap<>();

    YdTaskGroup getTaskGroupForTask(TaskRun task) {
        TaskGroupDirectiveMapper directiveMapper = new TaskGroupDirectiveMapper(task.config)
        processTaskGroups.compute(task.processor.name, (processName, existingTaskGroup) ->
                existingTaskGroup == null
                        ? addTaskGroup(processName, directiveMapper)
                        : checkExistingRunSpecification(processName, existingTaskGroup, directiveMapper)
        )
    }

    private static YdTaskGroup checkExistingRunSpecification(String processName, YdTaskGroup existingTaskGroup, TaskGroupDirectiveMapper directiveMapper) {

        RunSpecification runSpecification
        try {
            runSpecification = buildRunSpecification(directiveMapper)
        } catch (Exception ex) {
            throw new AbortOperationException(String.format("Unable to check existing task group run specification based on directives for process '%s' -> %s", processName, ex.message), ex)
        }
        if (existingTaskGroup.runSpecification != runSpecification) {
            log.warn("Changes to directive values for process '{}' will not be reflected in the associated YellowDog task group run specification as it has already been initialized", processName)
        }
        existingTaskGroup
    }

    void shutdown() {

        WorkRequirement workRequirement = workClient.getWorkRequirement(workRequirementId)

        if (workRequirement.getStatus().finished || workRequirement.getStatus() == WorkRequirementStatus.CANCELLING) {
            // nothing to do, work requirement is already finishing
            return;
        }

        if (allTasksFinished) {
            // if all tasks finished then set taskGroup.finishIfAllTasksFinished = true
            WorkRequirement updated = updateWorkRequirement { it.taskGroups.each { it.finishIfAllTasksFinished = true } }
            boolean noEmptyTaskGroups = !updated.taskGroups.any { it.taskSummary.taskCount == 0 }
            if (noEmptyTaskGroups) return
        }

        // otherwise, call cancel on work requirement
        workClient.cancelWorkRequirement(workRequirementId)
    }

    void cancelQuietly() {
        try {
            workClient.cancelWorkRequirement(workRequirementId)
        } catch (Throwable t) {
            log.error("Unable to cancel work requirement", t)
        }
    }

    private boolean isAllTasksFinished() {

        TaskSearch search = TaskSearch.builder()
                .workRequirementId(workRequirementId)
                .statuses(UNFINISHED_TASK_STATUSES)
                .build()

        workClient.getTasks(search)
                .slice(SliceReference.first(SliceReference.MIN_SIZE))
                .size() == 0
    }

    private WorkRequirement updateWorkRequirement(Consumer<WorkRequirement> update) {
        WorkRequirement workRequirement = workClient.getWorkRequirement(workRequirementId)
        update.accept(workRequirement)
        workClient.updateWorkRequirement(workRequirement)
    }

    static YdWorkRequirement addWorkRequirement(
            String name,
            String namespace,
            WorkRequirementConfig config,
            WorkClient workClient
    ) {
        WorkRequirement workRequirement = buildWorkRequirement(name, namespace, config)
        WorkRequirement submitted = submitWorkRequirement(workClient, workRequirement)
        new YdWorkRequirement(submitted.id, config, workClient)
    }

    private YdTaskGroup addTaskGroup(String processName, TaskGroupDirectiveMapper directiveMapper) {
        TaskGroup taskGroup = buildTaskGroup(processName, directiveMapper)
        WorkRequirement updatedWorkRequirement = updateWorkRequirement { it.taskGroups.push(taskGroup) }
        TaskGroup addedTaskGroup = updatedWorkRequirement.taskGroups.last()
        YdTaskGroup.create(addedTaskGroup, config.uploadTaskProcessOutput(), workClient)
    }

    private static WorkRequirement buildWorkRequirement(
            String name,
            String namespace,
            WorkRequirementConfig config
    ) {
        try {
            WorkRequirement.builder()
                    .name(name)
                    .namespace(namespace)
                    .priority(config?.priority)
                    .build()
        } catch (Exception ex) {
            throw new AbortOperationException(String.format("Unable to build work requirement -> %s", ex.message), ex)
        }
    }

    private static TaskGroup buildTaskGroup(String processName, TaskGroupDirectiveMapper directiveMapper) {
        try {
            TaskGroup.builder()
                    .name(sanitizeName(processName))
                    .tag(processName)
                    .finishIfAllTasksFinished(false)
                    .priority(directiveMapper.priority)
                    .completedTaskTtl(directiveMapper.completedTaskTtl ?: DEFAULT_COMPLETED_TASK_TTL)
                    .runSpecification(buildRunSpecification(directiveMapper))
                    .build()
        } catch (Exception ex) {
            throw new AbortOperationException(String.format("Unable to build task group for process '%s' -> %s", processName, ex.message), ex)
        }
    }

    private static RunSpecification buildRunSpecification(TaskGroupDirectiveMapper directiveMapper) {
        RunSpecification.builder()
                .taskType(Constants.YD_TASK_TYPE)
                .instanceTypes(directiveMapper.instanceTypes)
                .providers(directiveMapper.providers)
                .regions(directiveMapper.regions)
                .vcpus(directiveMapper.vcpus)
                .ram(directiveMapper.ram)
                .workerTags(directiveMapper.workerTags)
                .minWorkers(directiveMapper.minWorkers)
                .maxWorkers(directiveMapper.maxWorkers)
                .tasksPerWorker(directiveMapper.tasksPerWorker)
                .maximumTaskRetries(directiveMapper.maximumTaskRetries ?: DEFAULT_MAXIMUM_TASK_RETRIES)
                .exclusiveWorkers(directiveMapper.exclusiveWorkers)
                .build()
    }

    private static WorkRequirement submitWorkRequirement(WorkClient workClient, WorkRequirement workRequirement) {
        try {
            workClient.addWorkRequirement(workRequirement)
        } catch (Exception ex) {
            throw new AbortOperationException(String.format("Unable to add work requirement -> %s", ex.message), ex)
        }
    }
}
