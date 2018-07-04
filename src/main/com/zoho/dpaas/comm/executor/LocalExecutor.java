package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.LocalExecutorConf;
import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractDPAASExecutor;
import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;
import org.json.JSONObject;

import java.io.IOException;

public class LocalExecutor extends AbstractDPAASExecutor {

    public LocalExecutor(JSONObject executorConf) throws DPAASExecutorException {
        super(getSparkExecutorConf(executorConf));
    }

    static LocalExecutorConf getSparkExecutorConf(JSONObject executorConf) throws  DPAASExecutorException {
        try {
            return new ObjectMapper().readValue(executorConf.toString(),LocalExecutorConf.class);
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
