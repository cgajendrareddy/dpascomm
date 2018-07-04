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

public class HAExecutor extends AbstractDPAASExecutor{

    final List<DPAASExecutor> executorsList;
    DPAASExecutor currentExecutor;
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


    private static HAExecutorConf getExecutorConf(JSONObject executorConf) throws DPAASExecutorException {
        try {
            return new ObjectMapper().readValue(executorConf.toString(),HAExecutorConf.class);
        } catch (IOException e){
            throw new DPAASExecutorException(null,"Unable to initialize SparkClusterExecutor Conf",e);
        }
    }

    private static List<DPAASExecutor> getExecutors(HAExecutorConf executorConf)
    {
        //TODO return the list
        return null;
    }



    public class ExecutorFactory
    {
        private HAExecutor HAExecutor;
        private DPAASExecutor activeExecutor;
        private List<DPAASExecutor> dpasExecutors;
        public ExecutorFactory(HAExecutor HAExecutor)
        {
            this.HAExecutor = HAExecutor;
            this.dpasExecutors = new ArrayList<>(HAExecutor.executorsList);
            if(HAExecutor.currentExecutor!=null) {
                setActiveExecutor(HAExecutor.currentExecutor);
            }
        }
        private void setActiveExecutor(DPAASExecutor executor)
        {
            if(dpasExecutors !=null && dpasExecutors.contains(executor)) {
                this.activeExecutor=executor;
                dpasExecutors.remove(activeExecutor);
            }
        }
        private DPAASExecutor getActiveExecutor() throws HAExecutorException {
            if(activeExecutor!=null)
            {
                return activeExecutor;
            }
            setNextExecutorAsTheActiveExecutor();
            return activeExecutor;
        }

        private void setNextExecutorAsTheActiveExecutor() throws HAExecutorException {
            if(dpasExecutors.size()==0)
            {
                throw new HAExecutorException(HAExecutor,"Failed with all the dpasExecutors.");
            }
            setActiveExecutor(dpasExecutors.get(0));
        }
        private void executionFailed()
        {
            activeExecutor=null;
        }

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
                        HAExecutor.currentExecutor = getActiveExecutor();
                    }
                }

            }
            throw new HAExecutorException(HAExecutor,"Error occured");
        }

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
                        HAExecutor.currentExecutor = getActiveExecutor();
                    }
                }

            }
            throw new HAExecutorException(HAExecutor,"Error occured");
        }

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
                        HAExecutor.currentExecutor = getActiveExecutor();
                    }
                }

            }
            throw new HAExecutorException(HAExecutor,"Error occured");
        }

    }
}
