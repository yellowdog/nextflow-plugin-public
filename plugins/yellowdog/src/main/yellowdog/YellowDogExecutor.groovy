package yellowdog

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.exception.AbortOperationException
import nextflow.executor.Executor
import nextflow.fusion.FusionHelper
import nextflow.processor.TaskHandler
import nextflow.processor.TaskMonitor
import nextflow.processor.TaskPollingMonitor
import nextflow.processor.TaskRun
import nextflow.util.ServiceName
import org.pf4j.ExtensionPoint
import yellowdog.config.YellowDogConfig
import yellowdog.platform.YdFactory
import yellowdog.platform.YdTaskGroup
import yellowdog.platform.YdWorkRequirement
import yellowdog.platform.YdWorkerPool

@Slf4j
@ServiceName(Constants.EXECUTOR_NAME)
@CompileStatic
class YellowDogExecutor extends Executor implements ExtensionPoint {

    private YellowDogConfig config

    private YdFactory ydFactory

    private YdWorkRequirement ydWorkRequirement

    private YdWorkerPool ydWorkerPool

    @Override
    boolean isContainerNative() {
        isFusionEnabled()
    }

    @Override
    boolean isFusionEnabled() {
        FusionHelper.isFusionEnabled(session)
    }

    @Override
    protected TaskMonitor createTaskMonitor() {
        TaskPollingMonitor.create(
                getSession(),
                getName(),
                Constants.NF_DEFAULT_TASK_QUEUE_SIZE,
                Constants.NF_DEFAULT_TASKS_POLL_INTERVAL
        )
    }

    @Override
    TaskHandler createTaskHandler(TaskRun task) {
        checkContainerEnabled(task)
        new YellowDogTaskHandler(task, ydWorkRequirement)
    }

    @Override
    protected void register() {
        super.register()
        checkFusionEnabled()
        initConfig()
        initFactory()
        initWorkRequirement()
        if (config.workerPool) {
            try {
                initWorkerPool()
            } catch (Exception ex) {
                ydWorkRequirement.cancelQuietly()
                throw ex
            }
        }
    }

    @Override
    void shutdown() {
        super.shutdown()
        ydWorkerPool?.shutdown()
        ydWorkRequirement?.shutdown()
        ydFactory?.shutdown()
    }

    private static void checkContainerEnabled(TaskRun task) {
        if (!task.containerEnabled) {
            throw new AbortOperationException("YellowDog Executor requires tasks to be container based")
        }
    }

    private void checkFusionEnabled() {
        if (!fusionEnabled) {
            throw new AbortOperationException("YellowDog Executor requires Fusion FS to be enabled")
        }
    }

    private void initConfig() {
        config = new YellowDogConfig(session.config.yellowdog as Map)
    }

    private void initFactory() {
        ydFactory = YdFactory.create(
                config.platformUrl,
                config.apiKey.id,
                config.apiKey.secret,
                config.namespace
        )
    }

    private void initWorkRequirement() {
        ydWorkRequirement = ydFactory.addWorkRequirement(session.runName, config.workRequirement)
    }

    private void initWorkerPool() {
        ydWorkerPool = ydFactory.addWorkerPool(session.runName, config.workerPool)
    }
}
