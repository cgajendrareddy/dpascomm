package com.zoho.dpaas.comm.executor;

import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import org.json.JSONObject;

import java.util.List;

public class SparkClusterExecutor extends HAExecutor {


    public SparkClusterExecutor(JSONObject executorConf) throws ExecutorConfigException {
        this(getSparkExecutorList(executorConf));
    }

    private static List<Executor> getSparkExecutorList(JSONObject executorConf) {
        return null;
    }

    private SparkClusterExecutor(List<Executor> sparkClusterExecutors) throws ExecutorConfigException {
        super( sparkClusterExecutors);
    }
}
