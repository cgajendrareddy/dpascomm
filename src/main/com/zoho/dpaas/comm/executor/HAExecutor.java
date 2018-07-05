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
            return new ExecutorProxy(this).isResourcesAvailableFortheJob(jobType);
        }
        catch (HAExecutorException e)
        {
            throw new ExecutorException(this,e);
        }
    }

    @Override
    public String submit(String... appArgs) throws ExecutorException {
        try {
            return new ExecutorProxy(this).submit(appArgs);
        }
        catch (HAExecutorException e)
        {
            throw new ExecutorException(this,e);
        }

    }

    @Override
    public boolean killJob(String jobId) throws ExecutorException {
        try {
            return new ExecutorProxy(this).killJob(jobId);
        }
        catch (HAExecutorException e)
        {
            throw new ExecutorException(this,e);
        }
    }

    @Override
    public JobState getJobState(String jobId) throws ExecutorException {
        try {
            return new ExecutorProxy(this).getJobState(jobId);
        }
        catch (HAExecutorException e)
        {
            throw new ExecutorException(this,e);
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


    /**
     * Executor Proxy which takes care of trying the primary and standby executors and throw exception if none of them works out.
     */
    public class ExecutorProxy
    {
        /**
         * High availability executor instance
         */
        private HAExecutor HAExecutor;
        /**
         * active executor
         */
        private Executor activeExecutor;
        /**
         *list of executors
         */
        private List<Executor> dpasExecutors;

        /**
         * @param HAExecutor
         */
        public ExecutorProxy(HAExecutor HAExecutor)
        {
            this.HAExecutor = HAExecutor;
            this.dpasExecutors = new ArrayList<>(HAExecutor.executorsList);
            if(HAExecutor.currentActiveExecutor !=null) {
                setActiveExecutor(HAExecutor.currentActiveExecutor);
            }
        }

        /**
         * check whether the executor is part of the executor list and set it as current executor
         * @param executor
         */
        private void setActiveExecutor(Executor executor)
        {
            if(dpasExecutors !=null && dpasExecutors.contains(executor)) {
                this.activeExecutor=executor;
                dpasExecutors.remove(activeExecutor);
            }
        }

        /**
         * @return the active executor or throws exception if none of them fails.
         * @throws HAExecutorException
         */
        private Executor getActiveExecutor() throws HAExecutorException {
            if(activeExecutor!=null)
            {
                return activeExecutor;
            }
            setNextExecutorAsTheActiveExecutor();
            return activeExecutor;
        }

        /**
         * set the next executor in the executorlist as active executor.
         * @throws HAExecutorException
         */
        private void setNextExecutorAsTheActiveExecutor() throws HAExecutorException {
            if(dpasExecutors.size()==0)
            {
                throw new HAExecutorException(HAExecutor,"Failed with all the dpasExecutors.");
            }
            setActiveExecutor(dpasExecutors.get(0));
        }

        /**
         * execution failed.
         */
        private void executionFailed()
        {
            activeExecutor=null;
        }

        /**
         * @param appArgs
         * @return the jobid of the submission.
         * @throws HAExecutorException
         */
        public String submit(String... appArgs) throws HAExecutorException {
            boolean isSuccessfull=false;
            while (getActiveExecutor() != null) {
                try {
                    String toReturn=getActiveExecutor().submit(appArgs);
                    isSuccessfull=true;
                    return toReturn;
                } catch (ExecutorException ex) {
                    isSuccessfull=false;
                    executionFailed();
                }
                finally
                {
                    if(isSuccessfull) {
                        HAExecutor.currentActiveExecutor = getActiveExecutor();
                    }
                }

            }
            throw new HAExecutorException(HAExecutor,"Error occured");
        }

        /**
         * @param jobId
         * @return  true if job is killed
         * @throws HAExecutorException
         */
        public boolean killJob(String jobId) throws HAExecutorException {
            boolean isSuccessfull=false;
            while (getActiveExecutor() != null) {
                try {
                    boolean toReturn=getActiveExecutor().killJob(jobId);
                    isSuccessfull=true;
                    return toReturn;
                } catch (ExecutorException ex) {
                    isSuccessfull=false;
                    executionFailed();
                }
                finally
                {
                    if(isSuccessfull) {
                        HAExecutor.currentActiveExecutor = getActiveExecutor();
                    }
                }

            }
            throw new HAExecutorException(HAExecutor,"Error occured");
        }


        public boolean isResourcesAvailableFortheJob(String jobType) throws HAExecutorException {
            boolean isSuccessfull=false;
            while (getActiveExecutor() != null) {
                try {
                    boolean toReturn=getActiveExecutor().isResourcesAvailableFortheJob(jobType);
                    isSuccessfull=true;
                    return toReturn;
                } catch (ExecutorException ex) {
                    isSuccessfull=false;
                    executionFailed();
                }
                finally
                {
                    if(isSuccessfull) {
                        HAExecutor.currentActiveExecutor = getActiveExecutor();
                    }
                }

            }
            throw new HAExecutorException(HAExecutor,"Error occured");
        }

        /**
         * @param jobId
         * @return the job state.
         * @throws HAExecutorException
         */
        public JobState getJobState(String jobId) throws HAExecutorException {
            boolean isSuccessfull=false;
            while (getActiveExecutor() != null) {
                try {
                    JobState toReturn=getActiveExecutor().getJobState(jobId);
                    isSuccessfull=true;
                    return toReturn;
                } catch (ExecutorException ex) {
                    isSuccessfull=false;
                    executionFailed();
                }
                finally
                {
                    if(isSuccessfull) {
                        HAExecutor.currentActiveExecutor = getActiveExecutor();
                    }
                }

            }
            throw new HAExecutorException(HAExecutor,"Error occured");
        }

    }
}
