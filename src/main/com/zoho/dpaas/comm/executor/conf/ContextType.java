package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true) @ToString
public class ContextType {
    private String name;
    private Integer min;
    private Integer max;
    @JsonProperty(value = "configs")
    private Map<String,String> configs;
}
