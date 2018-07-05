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
        super(getSparkMasters(sparkClusterConfig));
    }

    /**
     * @param sparkMasters
     * @throws ExecutorConfigException
     */
    private SparkCluster(List<Executor> sparkMasters) throws ExecutorConfigException {
        super( sparkMasters);
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
        sparkClusterConfig.remove("masters");
        for(int i=0;i<masters.size();i++){
            JSONObject sparkCluster= new JSONObject(sparkClusterConfig.toString());
            JSONArray masterArray = new JSONArray();
            JSONObject master = new JSONObject(masters.get(i).toString());
            masterArray.put(master);
            sparkCluster.put("masters",masterArray);
            SparkMaster sparkMaster = new SparkMaster(sparkCluster);
            if(sparkMasters == null){
                sparkMasters = new ArrayList<>(5);
            }
            sparkMasters.add(sparkMaster);
        }
        return sparkMasters;
    }



    public static void main(String[] args) throws ExecutorConfigException {
        Executor executor = new SparkCluster(new JSONObject("{\"id\":2,\"name\":\"Cluster1\",\"disabled\":true,\"type\":\"SPARK_CLUSTER\",\"priority\":1,\"jobTypes\":[{\"jobType\":\"datasettransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},{\"jobType\":\"sampleextract\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},{\"jobType\":\"dsauditstatefile\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},{\"jobType\":\"rawdsaudittransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},{\"jobType\":\"erroraudit\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"}],\"masters\":[{\"host\":\"192.168.230.186\",\"port\":\"6066\",\"webUIPort\":\"8090\"},{\"host\":\"192.168.171.27\",\"port\":\"6066\",\"webUIPort\":\"8090\"}],\"sparkVersion\":\"2.2.1\",\"mainClass\":\"com.zoho.dpaas.processor.ZDExecutor\",\"appResource\":\"\",\"clusterMode\":\"spark\",\"httpScheme\":\"http\",\"appName\":\"SparkStandAlone\",\"config\":{\"spark.driver.supervise\":\"true\",\"spark.driver.memory\":\"2g\",\"spark.driver.cores\":2,\"spark.executor.cores\":2,\"spark.executor.memory\":\"2g\",\"spark.executor.instances\":2},\"environmentVariables\":{\"SPARK_ENV_LOADED\":\"1\"}}"));
        System.out.println("c");
    }
}
