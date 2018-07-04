package com.zoho.dpaas.comm.executor;

import com.zoho.dpaas.comm.executor.interfaces.ConfigProvider;
import lombok.Getter;
import org.json.JSONArray;

@Getter
public class ExecutorConfig implements ConfigProvider {
    private JSONArray executorConfig;
    @Override
    public void setDataProcessors(JSONArray config) {
        this.executorConfig = config;
    }

    public ExecutorConfig() {
        this.executorConfig = new JSONArray("[{\"id\":1,\"name\":\"Local\",\"disabled\":true,\"type\":\"SPARKLOCAL\",\"jobs\":[\"sampletransformation\",\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"samplepreview\",\"erroraudit\"],\"async\":true,\"mainClass\":\"com.zoho.callback.LocalCallBackHandler\"},{\"id\":2,\"spark.standalone.master\":\"spark://192.168.230.186:7077\",\"spark.standalone.server\":\"http://192.168.230.186:6066\",\"name\":\"Cluster1\",\"disabled\":true,\"type\":\"SPARKSDCLUSTER\",\"jobs\":[\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"erroraudit\"],\"host\":\"192.168.230.186\",\"port\":\"6066\",\"webUIPort\":\"8090\",\"sparkVersion\":\"2.2.1\",\"mainClass\":\"com.zoho.dpaas.processor.ZDExecutor\",\"appResource\":\"\",\"clusterMode\":\"spark\",\"httpScheme\":\"http\",\"appName\":\"SparkStandAlone\",\"params\":{\"spark.driver.supervise\":\"true\",\"spark.driver.memory\":\"2g\",\"spark.driver.cores\":2,\"spark.executor.cores\":2,\"spark.executor.memory\":\"2g\",\"spark.executor.instances\":2},\"environmentVariables\":{\"SPARK_ENV_LOADED\":\"1\"}},{\"id\":3,\"name\":\"Cluster2\",\"disabled\":true,\"type\":\"SPARKSDCLUSTER\",\"jobs\":[\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"erroraudit\"],\"host\":\"192.168.230.186\",\"port\":\"6066\",\"webUIPort\":\"8090\",\"sparkVersion\":\"2.2.1\",\"mainClass\":\"com.zoho.dpaas.processor.ZDExecutor\",\"appResource\":\"\",\"clusterMode\":\"spark\",\"httpScheme\":\"http\",\"appName\":\"SparkStandAlone\",\"params\":{\"spark.driver.supervise\":\"true\",\"spark.driver.memory\":\"2g\",\"spark.driver.cores\":2,\"spark.executor.cores\":2,\"spark.executor.memory\":\"2g\",\"spark.executor.instances\":2},\"environmentVariables\":{\"SPARK_ENV_LOADED\":\"1\"}},{\"id\":4,\"name\":\"SJS1\",\"disabled\":false,\"type\":\"SPARKSJS\",\"sparkClusterId\":2,\"jobs\":[\"sampletransformation\",\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"samplepreview\",\"erroraudit\"],\"sjsURL\":\"http://192.168.230.186:9090\",\"contextTypes\":[{\"name\":\"sample\",\"configs\":{\"spark.cores.max\":2,\"spark.executor.memory\":\"512m\"},\"min\":\"2\",\"max\":\"10\"},{\"name\":\"audit\",\"configs\":{\"spark.cores.max\":2,\"spark.executor.memory\":\"512m\"},\"min\":2,\"max\":10},{\"name\":\"initial_job\",\"configs\":{\"spark.cores.max\":2,\"spark.executor.memory\":\"1G\"},\"min\":\"2\",\"max\":\"10\"}]}]");
    }
}
