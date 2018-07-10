package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true)@ToString
public class HAExecutorConfig extends ExecutorConfig {
    @JsonProperty(value = "ids")//No I18N
    private List<Integer> ids;
}
