package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true)@ToString
public class SparkClusterConfig extends ExecutorConfig {
    public static List<String> params = Arrays.asList("spark.executor.instances","num-cpu-cores","num-cpu-cores","spark.executor.memory","spark.executor.cores","spark.driver.cores","spark.driver.memory","spark.driver.supervise");

    private String host;
    private Integer port;
    private Integer webUIPort;
    private String sparkVersion;
    private String mainClass;
    private String appResource;
    private String clusterMode;
    private String httpScheme;
    private String appName;
    private Map<String,String> config;
    private Map<String,String> environmentVariables;
    private List<String> jobs;
}
