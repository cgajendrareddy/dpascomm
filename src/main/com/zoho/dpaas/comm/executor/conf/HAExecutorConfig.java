package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true)
public class HAExecutorConfig extends ExecutorConfig {
    @JsonProperty(value = "ids")
    private List<Integer> ids;
}
