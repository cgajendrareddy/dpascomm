package com.zoho.dpaas.comm.executor;

import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import org.json.JSONObject;

import java.util.List;

public class SparkCluster extends HAExecutor {


    public SparkCluster(JSONObject executorConf) throws ExecutorConfigException {
        this(getSparkExecutorList(executorConf));
    }

    private static List<Executor> getSparkExecutorList(JSONObject executorConf) {
        return null;
    }

    private SparkCluster(List<Executor> sparkClusterExecutors) throws ExecutorConfigException {
        super( sparkClusterExecutors);
    }
}
