package com.zoho.dpaas.comm.executor;

import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import org.json.JSONObject;

import java.util.List;

public class SparkCluster extends HAExecutor {


    /**
     * @param sparkClusterConfig
     * @throws ExecutorConfigException
     */
    public SparkCluster(JSONObject sparkClusterConfig) throws ExecutorConfigException {
        this(getSparkMasters(sparkClusterConfig));
    }

    /**
     * @param sparkClusterConfig
     * @return the list of spark masters
     */
    private static List<Executor> getSparkMasters(JSONObject sparkClusterConfig) {
        return null;
    }

    /**
     * @param sparkMasters
     * @throws ExecutorConfigException
     */
    private SparkCluster(List<Executor> sparkMasters) throws ExecutorConfigException {
        super( sparkMasters);
    }
}
