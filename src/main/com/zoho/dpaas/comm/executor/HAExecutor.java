package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.HAExecutorConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.exception.HAExecutorException;
import com.zoho.dpaas.comm.executor.factory.ExecutorFactory;
import com.zoho.dpaas.comm.executor.interfaces.AbstractExecutor;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * High Availability Executor
 */
public class HAExecutor extends AbstractExecutor {

    /**
     * List of Executors
     */
    final List<Executor> executorsList;
    /**
     * current active Executor
     */
    Executor currentActiveExecutor;

    /**
     * @param executorConf
     * @throws ExecutorException
     */
    public HAExecutor(JSONObject executorConf) throws  ExecutorConfigException {
        super(getExecutorConf(executorConf));
        this.executorsList = getExecutors((HAExecutorConfig)getConf());
    }

    /**
     * @param executors
     * @throws ExecutorException
     * @throws ExecutorConfigException
     */
    public HAExecutor(List<Executor> executors) throws ExecutorConfigException {
        super(null);
        this.executorsList = executors;
    }

    public static void main(String[] args) throws ExecutorException, ExecutorConfigException {
        Executor executor = new HAExecutor(new JSONObject("{\"id\":1,\"name\":\"SPARKCLUSTER_HA1\",\"disabled\":true,\"type\":\"LOCAL_SPARK\",\"jobs\":[\"sampletransformation\",\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"samplepreview\",\"erroraudit\"],\"ids\":[2,3]}"));
        System.out.println("h");
    }

    @Override
    public boolean isResourcesAvailableFortheJob(String jobType) throws ExecutorException {
        try {
            if(currentActiveExecutor == null){
                throw new ExecutorException(this,"Invalid Executor");
            }
            return currentActiveExecutor.isResourcesAvailableFortheJob(jobType);
        }
        catch (ExecutorException e)
        {
            try {
                findCurrentActiveExecutor();
                return currentActiveExecutor.isResourcesAvailableFortheJob(jobType);
            }
            catch (HAExecutorException e1)
            {
                throw new ExecutorException(this,e1);
            }
        }
    }

    @Override
    public String submit(String... appArgs) throws ExecutorException {
        try {
            if(currentActiveExecutor == null){
                throw new ExecutorException(this,"Invalid Executor");
            }
            return currentActiveExecutor.submit(appArgs);
        }
        catch (ExecutorException e)
        {
            try {
                findCurrentActiveExecutor();
                return currentActiveExecutor.submit(appArgs);
            }
            catch (HAExecutorException e1)
            {
                throw new ExecutorException(this,e1);
            }
        }

    }

    @Override
    public boolean killJob(String jobId) throws ExecutorException {

        try {
            if(currentActiveExecutor == null){
                throw new ExecutorException(this,"Invalid Executor");
            }
            return currentActiveExecutor.killJob(jobId);
        }
        catch (ExecutorException e)
        {
            try {
                findCurrentActiveExecutor();
                return currentActiveExecutor.killJob(jobId);
            }
            catch (HAExecutorException e1)
            {
                throw new ExecutorException(this,e1);
            }
        }

    }

    @Override
    public JobState getJobState(String jobId) throws ExecutorException {
        try {
            if(currentActiveExecutor == null){
                throw new ExecutorException(this,"Invalid Executor");
            }
            return currentActiveExecutor.getJobState(jobId);
        }
        catch (ExecutorException e)
        {
            try {
                findCurrentActiveExecutor();
                return currentActiveExecutor.getJobState(jobId);
            }
            catch (HAExecutorException e1)
            {
                throw new ExecutorException(this,e1);
            }
        }
    }

    @Override
    public boolean isRunning() {

        if(currentActiveExecutor!=null && currentActiveExecutor.isRunning())
        {
            return true;
        }
        try {
                findCurrentActiveExecutor();
                return currentActiveExecutor.isRunning();
            }
            catch (HAExecutorException e1) {
                return false;
            }
    }


    /**
     * @param executorConf
     * @return the executor conf for this High availability Executor
     * @throws ExecutorException
     */
    private static HAExecutorConfig getExecutorConf(JSONObject executorConf) throws ExecutorConfigException {
        try {
            return new ObjectMapper().readValue(executorConf.toString(),HAExecutorConfig.class);
        } catch (IOException e){
            throw new ExecutorConfigException("Unable to initialize SparkCluster Conf",e);
        }
    }

    /**
     * @param executorConf
     * @return the list of executors configured
     */
    private static List<Executor> getExecutors(HAExecutorConfig executorConf) throws ExecutorConfigException {
        List<Executor> executors = null;
        List<Integer> ids = executorConf.getIds();
        for(int i=0;i<ids.size();i++){
            if(executors == null){
                executors = new ArrayList<>(4);
            }
            executors.add(ExecutorFactory.getExecutor(ids.get(i)));
        }
        return executors;
    }
    private void findCurrentActiveExecutor() throws HAExecutorException {
        boolean isSuccess = false;
        for(Executor executor:executorsList)
        {
            if(executor.isRunning())
            {
                currentActiveExecutor=executor;
                isSuccess = true;
                break;
            }
        }
        if(!isSuccess){
            throw new HAExecutorException(this,"Failed with all the Executors.");
        }
    }


}
