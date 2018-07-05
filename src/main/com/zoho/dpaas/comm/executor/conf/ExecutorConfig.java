package com.zoho.dpaas.comm.executor.conf;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.ExecutorType;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter @Setter @JsonIgnoreProperties(ignoreUnknown = true)@ToString
public class ExecutorConfig {
    private Integer id;
    private String name;
    private Boolean disabled;
    private ExecutorType type;
    private int priority;
}
