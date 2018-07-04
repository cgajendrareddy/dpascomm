package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.HAExecutorConf;
import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;
import com.zoho.dpaas.comm.executor.exception.HAExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractDPAASExecutor;
import com.zoho.dpaas.comm.executor.interfaces.DPAASExecutor;
import org.json.JSONObject;

import javax.print.attribute.standard.JobState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * High Availability Executor
 */
public class HAExecutor extends AbstractDPAASExecutor{

    /**
     * List of Executors
     */
    final List<DPAASExecutor> executorsList;
    /**
     * current active Executor
     */
    DPAASExecutor currentActiveExecutor;

    /**
     * @param executorConf
     * @throws DPAASExecutorException
     */
    public HAExecutor(JSONObject executorConf) throws DPAASExecutorException {
        super(getExecutorConf(executorConf));
        this.executorsList = getExecutors((HAExecutorConf)getConf());
    }

    @Override
    public String submit(String... appArgs) throws DPAASExecutorException {
        try {
            return new ExecutorFactory(this).submit(appArgs);
        }
        catch (HAExecutorException e)
        {
            throw new DPAASExecutorException(this,e);
        }

    }

    @Override
    public boolean killJob(String jobId) throws DPAASExecutorException {
        try {
            return new ExecutorFactory(this).killJob(jobId);
        }
        catch (HAExecutorException e)
        {
            throw new DPAASExecutorException(this,e);
        }
    }

    @Override
    public JobState getJobState(String jobId) throws DPAASExecutorException {
        try {
            return new ExecutorFactory(this).getJobState(jobId);
        }
        catch (HAExecutorException e)
        {
            throw new DPAASExecutorException(this,e);
        }
    }


    /**
     * @param executorConf
     * @return the executor conf for this High availability Executor
     * @throws DPAASExecutorException
     */
    private static HAExecutorConf getExecutorConf(JSONObject executorConf) throws DPAASExecutorException {
        try {
            return new ObjectMapper().readValue(executorConf.toString(),HAExecutorConf.class);
        } catch (IOException e){
            throw new DPAASExecutorException(null,"Unable to initialize SparkClusterExecutor Conf",e);
        }
    }

    /**
     * @param executorConf
     * @return the list of executors configured
     */
    private static List<DPAASExecutor> getExecutors(HAExecutorConf executorConf)
    {
        //TODO return the list
        return null;
    }


    /**
     * Executor Factory which takes care of trying the primary and standby executors and throw exception if none of them works out.
     */
    public class ExecutorFactory
    {
        /**
         * High availability executor instance
         */
        private HAExecutor HAExecutor;
        /**
         * active executor
         */
        private DPAASExecutor activeExecutor;
        /**
         *list of executors
         */
        private List<DPAASExecutor> dpasExecutors;

        /**
         * @param HAExecutor
         */
        public ExecutorFactory(HAExecutor HAExecutor)
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
        private void setActiveExecutor(DPAASExecutor executor)
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
        private DPAASExecutor getActiveExecutor() throws HAExecutorException {
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
                } catch (DPAASExecutorException ex) {
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
                } catch (DPAASExecutorException ex) {
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
                } catch (DPAASExecutorException ex) {
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
