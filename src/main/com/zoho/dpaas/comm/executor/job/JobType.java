package com.zoho.dpaas.comm.executor.job;

import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

import static com.zoho.dpaas.comm.executor.constants.ExecutorConstants.*;

@Setter@Getter @ToString
public class JobType  {
    private String jobType;
    private int minPool;
    private int maxPool;
    private int cores;
    private String mainClass;
    //TODO Add executr Instances for SparkCluster
    private String memory;

    /**
     * Get Params Map for Context Creation
     * @return
     * @throws ExecutorException
     */
    public Map<String,String>  getParamsForContextCreation() throws ExecutorException {
        Map<String,String> toReturn=new HashMap<>();
        if(this.getCores() ==0 || this.getMemory() == null){
            throw new ExecutorException(null,"Memory or cores cannot br null || 0");//No I18N
        }
        toReturn.put(CONTEXT_CORES,Integer.toString(this.getCores()));
        toReturn.put(CONTEXT_MEMORY,this.getMemory());
        return toReturn;
    }

    /**
     * Get Params Map for Executor Creation
     * @return
     */
    public Map<String,String>  getParamsForExecutorCreation(String clusterMode)
    {
        Map<String,String> toReturn=new HashMap<>();
        switch (clusterMode.toLowerCase()){
            case "standalone"://No I18N
            case "mesos"://No I18N
                toReturn.put(SPARK_CORES_MAX,Integer.toString(this.getCores()));
                break;
            case "yarn"://No I18N
                toReturn.put(EXECUTOR_CORES,Integer.toString(this.getCores()));
                break;
            default:
                toReturn.put(SPARK_CORES_MAX,Integer.toString(this.getCores()));
                break;
        }
        toReturn.put(EXECUTOR_MEMORY,this.getMemory());
        return toReturn;
    }
}
