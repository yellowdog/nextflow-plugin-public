package yellowdog

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.fusion.FusionAwareTask
import nextflow.fusion.FusionHelper
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.processor.TaskStatus
import nextflow.trace.TraceRecord
import yellowdog.platform.YdTask
import yellowdog.platform.YdTaskGroup
import yellowdog.platform.YdWorkRequirement

import java.util.function.Supplier

@Slf4j
@CompileStatic
class YellowDogTaskHandler extends TaskHandler implements FusionAwareTask {

    private final YdWorkRequirement ydWorkRequirement

    private YdTask ydTask

    YellowDogTaskHandler(TaskRun taskRun, YdWorkRequirement ydWorkRequirement) {
        super(taskRun)
        this.ydWorkRequirement = ydWorkRequirement;
    }

    @Override
    boolean checkIfRunning() {
        if (ydTask?.status?.inProgress) {
            status = TaskStatus.RUNNING
            return true
        }
        return false
    }

    @Override
    boolean checkIfCompleted() {
        if (ydTask?.status?.finished) {
            status = TaskStatus.COMPLETED
            task.exitStatus = ydTask.exitCode
            return true
        }
        return false
    }

    @Override
    void kill() {
        ydTask?.abort()
    }

    @Override
    void submit() {

        if (ydTask) {
            log.warn("YellowDogTaskHandler.submit() called again after task already created!")
            return
        }

        final launcher = fusionLauncher()

        // create the bash command wrapper and store in the task work dir
        launcher.build()

        final submit = fusionSubmitCli()
        final config = task.getContainerConfig()
        final containerOpts = task.config.getContainerOptions()
        final cmd = FusionHelper.runWithContainer(launcher, config, task.getContainer(), containerOpts, submit)
        final ydTaskGroup = ydWorkRequirement.getTaskGroupForTask(task)
        ydTask = ydTaskGroup.addTask(task.id, task.config, cmd)
        status = TaskStatus.SUBMITTED
    }

    /**
     * @return An {@link nextflow.trace.TraceRecord} instance holding task runtime information
     */
    @Override
    TraceRecord getTraceRecord() {
        final result = super.getTraceRecord()
        if (ydTask) {
            result.put('native_id', ydTask.id)
        }
        return result
    }
}
