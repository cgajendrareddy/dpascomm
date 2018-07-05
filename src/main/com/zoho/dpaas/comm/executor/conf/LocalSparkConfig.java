package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true)
public class LocalSparkConfig extends ExecutorConfig {
    private List<String> jobs;
    private Boolean async;
    private String mainClass;
}
