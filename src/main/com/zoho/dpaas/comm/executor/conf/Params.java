package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true)
public class Params {
    private String name;
    private String value;
}
