package com.zoho.dpaas.comm.executor.conf;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter @Getter
public class LocalExecutorConf extends DPAASExecutorConf {
    private List<String> jobs;
    private Boolean async;
    private String mainClass;
}
