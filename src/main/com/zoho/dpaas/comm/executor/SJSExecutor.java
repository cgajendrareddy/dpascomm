package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.SJSExecutorConf;
import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractDPAASExecutor;
import org.json.JSONObject;

import javax.print.attribute.standard.JobState;
import java.io.IOException;

public class SJSExecutor extends AbstractDPAASExecutor {

    public SJSExecutor(JSONObject executorConf) throws DPAASExecutorException {
        super(getSparkExecutorConf(executorConf));
    }

    static SJSExecutorConf getSparkExecutorConf(JSONObject executorConf) throws  DPAASExecutorException {
        try {
            return new ObjectMapper().readValue(executorConf.toString(),SJSExecutorConf.class);
        } catch (IOException e){
            throw new DPAASExecutorException(null,"Unable to initialize SparkClusterExecutor Conf",e);
        }
    }

    @Override
    public String submit(String... appArgs) throws DPAASExecutorException {
        return null;
    }

    @Override
    public boolean killJob(String jobId) throws DPAASExecutorException {
        return false;
    }

    @Override
    public JobState getJobState(String jobId) throws DPAASExecutorException {
        return null;
    }
}
