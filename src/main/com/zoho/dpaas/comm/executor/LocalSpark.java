package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.LocalSparkConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractDPAASExecutor;
import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;
import org.json.JSONObject;

import java.io.IOException;

public class LocalSpark extends AbstractDPAASExecutor {

    /**
     * @param executorConf
     * @throws ExecutorConfigException
     */
    public LocalSpark(JSONObject executorConf) throws ExecutorConfigException {
        super(getSparkExecutorConf(executorConf));
    }

    /**
     * @param executorConf
     * @return the localsparkConfig object
     * @throws ExecutorConfigException
     */
    static LocalSparkConfig getSparkExecutorConf(JSONObject executorConf) throws ExecutorConfigException {
        try {
            return new ObjectMapper().readValue(executorConf.toString(),LocalSparkConfig.class);
        } catch (IOException e){
            throw new ExecutorConfigException("Unable to initialize SparkCluster Conf",e);
        }
    }

    @Override
    public String submit(String... appArgs) throws ExecutorException {
        return null;
    }

    @Override
    public boolean killJob(String jobId) throws ExecutorException {
        return false;
    }

    @Override
    public JobState getJobState(String jobId) throws ExecutorException {
        return null;
    }
}
