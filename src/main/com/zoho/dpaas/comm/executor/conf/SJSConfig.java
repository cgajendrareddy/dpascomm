package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true)
public class SJSConfig extends ExecutorConfig {
    private List<String> jobs;
    private String sjsURL;
    private Integer sparkClusterId;
    private List<ContextType> contextTypes;
    private Map<String,String> config;

    private void addConfig(String configName,String configValue){
        if(this.config == null){
            this.config = new HashMap<>(5);
        }
        this.config.put(configName,configValue);
    }

}
