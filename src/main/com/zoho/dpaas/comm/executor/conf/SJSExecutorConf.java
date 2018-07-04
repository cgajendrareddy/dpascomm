package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true)
public class SJSExecutorConf extends DPAASExecutorConf {
    private List<String> jobs;
    private String sjsURL;
    private Integer sparkClusterId;
    private  List<ContextType> contextTypes;

}
