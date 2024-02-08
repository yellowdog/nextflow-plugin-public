package yellowdog.platform

import co.yellowdog.platform.clients.scheduler.WorkClient
import co.yellowdog.platform.model.Task
import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import groovy.transform.VisibilityOptions
import groovy.transform.options.Visibility
import yellowdog.Constants

import java.time.Instant

@VisibilityOptions(constructor = Visibility.PACKAGE_PRIVATE)
@TupleConstructor(includeFields = true)
@CompileStatic
class SingleTaskCachingFetcher {

    private final String taskId

    private final WorkClient workClient

    private Task task = null

    private long lastFetchedTime = 0

    Task getTask() {
        long now = Instant.now().toEpochMilli()
        if (task == null || fetchIntervalEndedBefore(now)) {
            task = workClient.getTask(taskId)
            lastFetchedTime = now
        }
        task
    }

    private boolean fetchIntervalEndedBefore(long now) {
        (now - lastFetchedTime) > Constants.YD_TASK_FETCH_INTERVAL.toMillis()
    }
}
