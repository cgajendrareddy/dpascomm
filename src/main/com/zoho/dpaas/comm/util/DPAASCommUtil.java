//$Id$
package com.zoho.dpaas.comm.util;

/**
 * Created by elam-4191 on 5/22/2017.
 *
 */
public class DPAASCommUtil {

    public enum JobState {
        INPROGRESS,
        FAILED,
        KILLED,
        SUCCESS;
    }

    public enum ExecutorType {
        SPARK_SJS, SPARK_CLUSTER, LOCAL_SPARK
    }
}
