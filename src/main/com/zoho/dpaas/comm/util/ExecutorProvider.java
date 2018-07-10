package com.zoho.dpaas.comm.util;

import com.zoho.dpaas.comm.executor.interfaces.ExecutorConfigProvider;
import org.json.JSONArray;
import org.json.JSONObject;

public class ExecutorProvider implements ExecutorConfigProvider {

    //TODO Dummy remove it
    private JSONObject executorConfig = new JSONObject("{\"executors\":[{\"id\":1,\"name\":\"Local\",\"disabled\":true,\"type\":\"LOCAL_SPARK\",\"priority\":2,\"classPath\":\"com.zoho.dpaas.comm.executor.LocalSpark\",\"jobTypes\":{\"datasettransformation\":{\"jobType\":\"datasettransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"com.zoho.dpaas.comm.executor.LocalSpark\"},\"sampleextract\":{\"jobType\":\"sampleextract\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"com.zoho.dpaas.comm.executor.LocalSpark\"},\"dsauditstatefile\":{\"jobType\":\"dsauditstatefile\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2g\",\"classPath\":\"com.zoho.dpaas.comm.executor.LocalSpark\"},\"rawdsaudittransformation\":{\"jobType\":\"rawdsaudittransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"com.zoho.dpaas.comm.executor.LocalSpark\"},\"erroraudit\":{\"jobType\":\"erroraudit\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"com.zoho.dpaas.comm.executor.LocalSpark\"}}},{\"id\":2,\"name\":\"Cluster1\",\"disabled\":true,\"type\":\"SPARK_CLUSTER\",\"priority\":1,\"jobTypes\":{\"datasettransformation\":{\"jobType\":\"datasettransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"com.zoho.dpaas.processor.ZDExecutor\"},\"sampleextract\":{\"jobType\":\"sampleextract\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"com.zoho.dpaas.processor.ZDExecutor\"},\"dsauditstatefile\":{\"jobType\":\"dsauditstatefile\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2g\",\"classPath\":\"com.zoho.dpaas.processor.ZDExecutor\"},\"rawdsaudittransformation\":{\"jobType\":\"rawdsaudittransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"com.zoho.dpaas.processor.ZDExecutor\"},\"erroraudit\":{\"jobType\":\"erroraudit\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"com.zoho.dpaas.processor.ZDExecutor\"}},\"masters\":[{\"host\":\"192.168.230.186\",\"port\":\"6066\",\"webUIPort\":\"8090\"},{\"host\":\"192.168.171.27\",\"port\":\"6066\",\"webUIPort\":\"8090\"}],\"sparkVersion\":\"2.2.1\",\"classPath\":\"com.zoho.dpaas.processor.ZDExecutor\",\"appResource\":\"\",\"clusterMode\":\"spark\",\"httpScheme\":\"http\",\"appName\":\"SparkStandAlone\",\"config\":{\"spark.driver.supervise\":\"true\",\"spark.driver.memory\":\"2g\",\"spark.driver.cores\":2,\"spark.executor.instances\":2,\"spark.driver.extraJavaOptions\":\"Dorg.xerial.snappy.tempdir=/home/sas/zdpas/spark/snappydata\"},\"environmentVariables\":{\"SPARK_ENV_LOADED\":\"1\"}},{\"id\":3,\"name\":\"SJS1\",\"disabled\":false,\"type\":\"SPARK_SJS\",\"priority\":3,\"sparkClusterId\":2,\"sjsURL\":\"http://192.168.230.186:9090\",\"classPath\":\"spark.jobserver.TestSqlJob\",\"context-factory\":\"spark.jobserver.context.SQLContextFactory\",\"config\":{\"appName\":\"snap\"},\"jobTypes\":{\"datasettransformation\":{\"jobType\":\"datasettransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"spark.jobserver.TestSqlJob\"},\"sampleextract\":{\"jobType\":\"sampleextract\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"spark.jobserver.TestSqlJob\"},\"dsauditstatefile\":{\"jobType\":\"dsauditstatefile\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2g\",\"classPath\":\"spark.jobserver.TestSqlJob\"},\"rawdsaudittransformation\":{\"jobType\":\"rawdsaudittransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"spark.jobserver.TestSqlJob\"},\"erroraudit\":{\"jobType\":\"erroraudit\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"spark.jobserver.TestSqlJob\"}}},{\"id\":5,\"name\":\"SJS_HA1\",\"disabled\":true,\"type\":\"SPARK_SJS\",\"jobTypes\":{\"datasettransformation\":{\"jobType\":\"datasettransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"spark.jobserver.TestSqlJob\"},\"sampleextract\":{\"jobType\":\"sampleextract\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"spark.jobserver.TestSqlJob\"},\"dsauditstatefile\":{\"jobType\":\"dsauditstatefile\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2g\",\"classPath\":\"spark.jobserver.TestSqlJob\"},\"rawdsaudittransformation\":{\"jobType\":\"rawdsaudittransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"spark.jobserver.TestSqlJob\"},\"erroraudit\":{\"jobType\":\"erroraudit\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\",\"classPath\":\"spark.jobserver.TestSqlJob\"}},\"ids\":[2,3]}]}");//No I18N

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
