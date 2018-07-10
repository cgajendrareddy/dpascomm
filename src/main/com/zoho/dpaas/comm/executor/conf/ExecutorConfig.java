package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.zoho.dpaas.comm.executor.job.JobType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

@Getter @Setter @JsonIgnoreProperties(ignoreUnknown = true)@ToString
public class ExecutorConfig {
    private Integer id;
    private String executorClass;
    private String name;
    private Boolean disabled;
    private String type;
    private int priority;
    private Map<String,JobType> jobTypes;
}
