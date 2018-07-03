package com.zoho.dpaas.comm.executor.conf;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.ExecutorType;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@Getter @Setter @JsonIgnoreProperties(ignoreUnknown = true)
public class DPAASExecutorConf {
    private Integer id;
    private String name;
    private Boolean disabled;
    private ExecutorType type;
}
