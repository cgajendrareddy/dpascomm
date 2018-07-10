package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true)@ToString
public class SJSConfig extends ExecutorConfig {
    private String sjsURL;
    private Integer sparkClusterId;
    private Map<String,String> config;
    private String classPath;
    @JsonProperty(value = "context-factory")//No I18N
    private String contextFactory;

    /**
     * Add configurations to config Map
     * @param configName
     * @param configValue
     */
    public void addConfig(String configName,String configValue){
        if(this.config == null){
            this.config = new HashMap<>(5);
        }
        this.config.put(configName,configValue);
    }

}
