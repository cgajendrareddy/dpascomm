package com.zoho.dpaas.comm.util;

import com.zoho.dpaas.comm.executor.interfaces.ExecutorConfigProvider;
import org.json.JSONArray;
import org.json.JSONObject;

public class ExecutorProvider implements ExecutorConfigProvider {

    //TODO Dummy remove it
    private JSONObject executorConfig = new JSONObject("{\"executors\":[{\"id\":1,\"name\":\"Local\",\"disabled\":true,\"type\":\"LOCAL_SPARK\",\"priority\":2,\"job\":[\"sampletransformation\",\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"samplepreview\",\"erroraudit\"],\"jobTypes\":{\"sampletransformation\":{\"jobType\":\"sampletransformation\",\"minPool\":2,\"maxPool\":3},\"datasettransformation\":{\"jobType\":\"datasettransformation\",\"minPool\":2,\"maxPool\":3},\"sampleextract\":{\"jobType\":\"sampleextract\",\"minPool\":2,\"maxPool\":3},\"dsauditstatefile\":{\"jobType\":\"dsauditstatefile\",\"minPool\":2,\"maxPool\":3},\"rawdsaudittransformation\":{\"jobType\":\"rawdsaudittransformation\",\"minPool\":2,\"maxPool\":3},\"samplepreview\":{\"jobType\":\"samplepreview\",\"minPool\":2,\"maxPool\":3},\"erroraudit\":{\"jobType\":\"erroraudit\",\"minPool\":2,\"maxPool\":3}},\"className\":\"com.zoho.dpaas.comm.executor.LocalSpark\"},{\"id\":2,\"name\":\"Cluster1\",\"disabled\":true,\"type\":\"SPARK_CLUSTER\",\"priority\":1,\"job\":[\"sampletransformation\",\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"samplepreview\",\"erroraudit\"],\"jobTypes\":{\"datasettransformation\":{\"jobType\":\"datasettransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},\"sampleextract\":{\"jobType\":\"sampleextract\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},\"dsauditstatefile\":{\"jobType\":\"dsauditstatefile\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2g\"},\"rawdsaudittransformation\":{\"jobType\":\"rawdsaudittransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},\"erroraudit\":{\"jobType\":\"erroraudit\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"}},\"masters\":[{\"host\":\"172.20.14.139\",\"port\":\"6066\",\"webUIPort\":\"8090\"},{\"host\":\"172.20.15.241\",\"port\":\"6066\",\"webUIPort\":\"8090\"},{\"host\":\"192.168.171.27\",\"port\":\"6066\",\"webUIPort\":\"8090\"}],\"sparkVersion\":\"2.2.1\",\"mainClass\":\"com.zoho.dpaas.processor.ZDExecutor\",\"appResource\":\"\",\"clusterMode\":\"spark\",\"httpScheme\":\"http\",\"appName\":\"SparkStandAlone\",\"config\":{\"spark.driver.supervise\":\"true\",\"spark.driver.memory\":\"2g\",\"spark.driver.cores\":2,\"spark.executor.instances\":2},\"environmentVariables\":{\"SPARK_ENV_LOADED\":\"1\"}},{\"id\":3,\"name\":\"SJS1\",\"disabled\":false,\"type\":\"SPARK_SJS\",\"priority\":3,\"sparkClusterId\":2,\"sjsURL\":\"http://172.20.15.241:9090\",\"job\":[\"sampletransformation\",\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"samplepreview\",\"erroraudit\"],\"config\":{\"classPath\":\"spark.jobserver.TestSqlJob\",\"context-factory\":\"spark.jobserver.context.SQLContextFactory\",\"appName\":\"snap\"},\"jobTypes\":{\"datasettransformation\":{\"jobType\":\"datasettransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},\"sampleextract\":{\"jobType\":\"sampleextract\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},\"dsauditstatefile\":{\"jobType\":\"dsauditstatefile\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2g\"},\"rawdsaudittransformation\":{\"jobType\":\"rawdsaudittransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},\"erroraudit\":{\"jobType\":\"erroraudit\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"}}}]}");

    public ExecutorProvider(){
    }

    public ExecutorProvider(JSONObject executorConfig){
        this.executorConfig = executorConfig;
    }

    @Override
    public JSONObject getExecutorConfig(int id) {
        JSONArray executors = executorConfig.optJSONArray("executors");
        for(int i=0;i<executors.length();i++){
            if(executors.getJSONObject(i).optInt("id") == id){
                return executors.getJSONObject(i);
            }
        }
        return null;
    }

    @Override
    public JSONObject getExecutorConfigs() {
        return executorConfig;
    }
}
