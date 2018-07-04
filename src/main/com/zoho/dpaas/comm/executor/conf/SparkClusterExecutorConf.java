package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true)
public class SparkClusterExecutorConf extends DPAASExecutorConf {
    private String host;
    private Integer port;
    private Integer webUIPort;
    private String sparkVersion;
    private String mainClass;
    private String appResource;
    private String clusterMode;
    private String httpScheme;
    private String appName;
    private Config params;
    private Map<String,String> environmentVariables;
    private List<String> jobs;
}
