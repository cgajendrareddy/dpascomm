package com.zoho.dpaas.comm.executor.factory;

import com.zoho.dpaas.comm.executor.ExecutorConfig;
import com.zoho.dpaas.comm.executor.LocalExecutor;
import com.zoho.dpaas.comm.executor.SJSExecutor;
import com.zoho.dpaas.comm.executor.SparkClusterExecutor;
import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractDPAASExecutor;
import org.json.JSONArray;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.ExecutorType;

public class ExecutorFactory {

    public static AbstractDPAASExecutor getExecutor(Integer id) throws DPAASExecutorException {
        ExecutorConfig executorConfig = new ExecutorConfig();
        if(executorConfig.getExecutorConfig()!=null){
            JSONArray executors = executorConfig.getExecutorConfig();
            for(int i=0;i<executors.length();i++){
                if(executors.getJSONObject(i).optInt("id") == id){
                    if(ExecutorType.SPARKLOCAL.toString().equals(executors.getJSONObject(i).optString("type"))) {
                        return new LocalExecutor(executors.getJSONObject(i));
                    } else if(ExecutorType.SPARKSDCLUSTER.toString().equals(executors.getJSONObject(i).optString("type"))) {
                        return new SparkClusterExecutor(executors.getJSONObject(i));
                    } else if(ExecutorType.SPARKSJS.toString().equals(executors.getJSONObject(i).optString("type"))) {
                        return new SJSExecutor(executors.getJSONObject(i));
                    } else {
                        throw new DPAASExecutorException(null,"Invalid executor type "+executors.getJSONObject(i).optString("type")+" for executor ID "+id);
                    }
                }
            }
        }
        throw new DPAASExecutorException(null,"Executors Not Found");
    }
}
