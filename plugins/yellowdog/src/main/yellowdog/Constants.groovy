package yellowdog

import groovy.transform.CompileStatic
import nextflow.util.Duration

@CompileStatic
class Constants {

    static final String EXECUTOR_NAME = "yellowdog"

    static final int NF_DEFAULT_TASK_QUEUE_SIZE = 1000

    static final Duration NF_DEFAULT_TASKS_POLL_INTERVAL = Duration.of('5 sec')

    static final Duration YD_TASK_FETCH_INTERVAL = Duration.of('15 sec')

    static final String YD_TASK_TYPE = "nf-fusion"
}
