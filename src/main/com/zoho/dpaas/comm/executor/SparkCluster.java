package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.Master;
import com.zoho.dpaas.comm.executor.conf.SparkClusterConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SparkCluster extends HAExecutor {

    public static final String MASTERS = "masters";//No I18N
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
            throw new ExecutorConfigException("Unable to initialize SparkCluster Conf",e);//No I18N
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
        if(clusterConfig.has(MASTERS)){
            clusterConfig.remove(MASTERS);
        } else {
            throw new ExecutorConfigException("Masters cannot be null in Executor Id :"+sparkClusterConf.getId());//No I18N
        }
        for(Master master:masters){
            JSONObject sparkCluster= new JSONObject(clusterConfig.toString());
            JSONObject masterJSON = new JSONObject(master.toString());
            sparkCluster.put(MASTERS,new JSONArray().put(masterJSON));
            SparkMaster sparkMaster = new SparkMaster(sparkCluster);
            if(sparkMasters == null){
                sparkMasters = new ArrayList<>(5);
            }
            sparkMasters.add(sparkMaster);
        }
        return sparkMasters;
    }

    public static void main(String[] args) throws ExecutorConfigException, ExecutorException {
        SparkCluster cluster = new SparkCluster(new JSONObject("{\"id\":2,\"name\":\"Cluster1\",\"disabled\":true,\"type\":\"SPARK_CLUSTER\",\"priority\":1,\"jobTypes\":{\"datasettransformation\":{\"jobType\":\"datasettransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":1,\"memory\":\"1G\"},\"sampleextract\":{\"jobType\":\"sampleextract\",\"minPool\":2,\"maxPool\":3,\"cores\":1,\"memory\":\"1G\"},\"dsauditstatefile\":{\"jobType\":\"dsauditstatefile\",\"minPool\":2,\"maxPool\":3,\"cores\":1,\"memory\":\"1G\"},\"rawdsaudittransformation\":{\"jobType\":\"rawdsaudittransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":1,\"memory\":\"1G\"},\"erroraudit\":{\"jobType\":\"erroraudit\",\"minPool\":2,\"maxPool\":3,\"cores\":1,\"memory\":\"1G\"}},\"masters\":[{\"host\":\"192.168.230.186\",\"port\":\"6066\",\"webUIPort\":\"8090\"},{\"host\":\"192.168.171.27\",\"port\":\"6066\",\"webUIPort\":\"8090\"}],\"sparkVersion\":\"2.2.1\",\"classPath\":\"org.apache.spark.examples.TestJob\",\"appResource\":\"http://172.24.103.8:8000/testJar.jar\",\"jars\":[],\"clusterMode\":\"spark\",\"httpScheme\":\"http\",\"appName\":\"SparkStandAlone\",\"config\":{\"spark.driver.memory\":\"1g\",\"spark.driver.cores\":1,\"spark.executor.instances\":1},\"environmentVariables\":{\"SPARK_ENV_LOADED\":\"1\"}}"));//No I18N
        cluster.submit("dsauditstatefile",new String[]{"dsauditstatefile@4 300000"});//No I18N
        System.out.println("h");//No I18N
    }
}
