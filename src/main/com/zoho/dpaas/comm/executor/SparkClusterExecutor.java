package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.SparkClusterExecutorConf;
import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractDPAASExecutor;
import org.json.JSONObject;

import java.io.IOException;

public class SparkClusterExecutor extends AbstractDPAASExecutor {


    public SparkClusterExecutor(JSONObject executorConf) throws DPAASExecutorException {
        super(getSparkExecutorConf(executorConf));
    }

    static SparkClusterExecutorConf getSparkExecutorConf(JSONObject executorConf) throws  DPAASExecutorException {
        try {
         return new ObjectMapper().readValue(executorConf.toString(),SparkClusterExecutorConf.class);
        } catch (IOException e){
            throw new DPAASExecutorException(null,"Unable to initialize SparkClusterExecutor Conf",e);
        }
    }
}