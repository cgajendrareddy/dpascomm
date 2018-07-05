//$Id$
package com.zoho.dpaas.comm.util;

import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.job.JobType;

/**
 * Created by elam-4191 on 5/22/2017.
 *
 */
public class DPAASCommUtil {

    public enum JobState {
        SUBMITTED("INPROGRESS"),
        RUNNING("INPROGRESS"),
        FINISHED("SUCCESS"),
        RELAUNCHING("INPROGRESS"),
        UNKNOWN("FAILED"),
        KILLED("KILLED"),
        FAILED("FAILED"),
        ERROR("FAILED"),
        QUEUED("INPROGRESS"),
        RETRYING("INPROGRESS"),
        NOT_FOUND("FAILED");

        JobState(String failed) {
        }
    }

    public enum ExecutorType {
        SPARK_SJS, SPARK_CLUSTER, LOCAL_SPARK
    }
}
