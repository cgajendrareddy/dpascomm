package com.zoho.dpaas.comm.executor.job;

import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

@Setter@Getter @ToString
public class JobType {
    public static final String CONTEXT_CORES="num-cpu-cores";
    public static final String CONTEXT_MEMORY="memory-per-node";
    public static final String EXECUTOR_CORES="spark.executor.cores";
    public static final String EXECUTOR_MEMORY="spark.executor.memory";
    private String jobType;
    private int minPool;
    private int maxPool;
    private int cores;
    //TODO Add executr Instances for SparkCluster
    private String memory;

    public Map<String,String>  getParamsForContextCreation() throws ExecutorException {
        Map<String,String> toReturn=new HashMap<>();
        if(this.getCores() ==0 || this.getMemory() == null){
            throw new ExecutorException(null,"Memory or cores cannot br null || 0");
        }
        toReturn.put(CONTEXT_CORES,Integer.toString(this.getCores()));
        toReturn.put(CONTEXT_MEMORY,this.getMemory());
        return toReturn;
    }

    public Map<String,String>  getParamsForExecutorCreation()
    {
        Map<String,String> toReturn=new HashMap<>();
        toReturn.put(EXECUTOR_CORES,Integer.toString(this.getCores()));
        toReturn.put(EXECUTOR_MEMORY,this.getMemory());
        return toReturn;
    }
}
