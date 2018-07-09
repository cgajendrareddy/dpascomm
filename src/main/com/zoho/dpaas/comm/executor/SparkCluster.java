package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.Master;
import com.zoho.dpaas.comm.executor.conf.SparkClusterConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SparkCluster extends HAExecutor {


    /**
     * @param sparkClusterConfig
     * @throws ExecutorConfigException
     */
    public SparkCluster(JSONObject sparkClusterConfig) throws ExecutorConfigException {
        super(getSparkMasters(sparkClusterConfig),getSparkExecutorConf(sparkClusterConfig));
    }

    /**
     * @param sparkMasters
     * @throws ExecutorConfigException
     */
    private SparkCluster(List<Executor> sparkMasters,SparkClusterConfig clusterConfig) throws ExecutorConfigException {
        super( sparkMasters,clusterConfig);
    }

    /**
     * @param sparkMasterConfig
     * @return
     * @throws ExecutorConfigException
     */
    static SparkClusterConfig getSparkExecutorConf(JSONObject sparkMasterConfig) throws ExecutorConfigException {
        try {
            return new ObjectMapper().readValue(sparkMasterConfig.toString(),SparkClusterConfig.class);
        } catch (IOException e){
            throw new ExecutorConfigException("Unable to initialize SparkCluster Conf",e);
        }
    }

    /**
     * @param sparkClusterConfig
     * @return the list of spark masters
     */
    private static List<Executor> getSparkMasters(JSONObject sparkClusterConfig) throws ExecutorConfigException {

        List<Executor> sparkMasters = null;
        SparkClusterConfig sparkClusterConf=getSparkExecutorConf(sparkClusterConfig);
        List<Master> masters = sparkClusterConf.getMasters();
        JSONObject clusterConfig = new JSONObject(sparkClusterConfig.toString());
        if(clusterConfig.has("masters")){
            clusterConfig.remove("masters");
        } else {
            throw new ExecutorConfigException("Masters cannot be null in Executor Id :"+sparkClusterConf.getId());
        }
        for(Master master:masters){
            JSONObject sparkCluster= new JSONObject(clusterConfig.toString());
            JSONObject masterJSON = new JSONObject(master.toString());
            sparkCluster.put("masters",new JSONArray().put(masterJSON));
            SparkMaster sparkMaster = new SparkMaster(sparkCluster);
            if(sparkMasters == null){
                sparkMasters = new ArrayList<>(5);
            }
            sparkMasters.add(sparkMaster);
        }
        return sparkMasters;
    }
}
