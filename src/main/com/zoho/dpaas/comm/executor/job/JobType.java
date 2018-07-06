package com.zoho.dpaas.comm.executor.job;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Setter@Getter @ToString
public class JobType {
    private String jobType;
    private int minPool;
    private int maxPool;
    private int cores;
    private String memory;

    public Map<String,String>  getParamsForContextCreation()
    {
        Map<String,String> toReturn=new HashMap<>();
        return toReturn;
    }
}
