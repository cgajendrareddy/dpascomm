package com.zoho.dpaas.comm.executor;

import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;
import com.zoho.dpaas.comm.executor.exception.DPAASHAExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractDPAASExecutor;
import com.zoho.dpaas.comm.executor.interfaces.DPAASExecutor;

import javax.print.attribute.standard.JobState;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

public class HaExecutor extends AbstractDPAASExecutor {


    Queue<DPAASExecutor> executorQueue;
    Queue<DPAASExecutor> failedExecutorQueue =new ArrayBlockingQueue<>(2);
    DPAASExecutor currentExecutor;
    boolean isAlive;

    public HaExecutor(Queue<DPAASExecutor> executors) throws DPAASHAExecutorException {
        //TODO for now
        super(null);
        if(executors!=null &&executors.size()>=2) {
            this.executorQueue=executors;
            currentExecutor=executorQueue.element();
        }
        else
        {
            throw new DPAASHAExecutorException(null,new InstantiationException("Atleast two executors should be passed :"+executors));
        }
    }



    @Override
    public String submit(String... appArgs) throws DPAASExecutorException {
        try {
            DPAASExecutor executor;
            while((executor= getExecutor())!=null) {
                try {
                    return executor.submit(appArgs);
                } catch (DPAASExecutorException e1) {
                    isAlive =false;
                }
            }
        }
        catch (DPAASHAExecutorException e)
        {
            throw new DPAASExecutorException(e.getExecutor(),e);
        }
        throw new  DPAASExecutorException(this,"Failed");
    }

    @Override
    public boolean killJob(String jobId) throws DPAASExecutorException {
        try {
            DPAASExecutor executor;
            while((executor= getExecutor())!=null) {
                try {
                    return executor.killJob(jobId);
                } catch (DPAASExecutorException e1) {
                    isAlive =false;
                }
            }
        }
        catch (DPAASHAExecutorException e)
        {
            throw new DPAASExecutorException(e.getExecutor(),e);
        }
        throw new  DPAASExecutorException(this,"Failed");
    }

    @Override
    public JobState getJobState(String jobId) throws DPAASExecutorException {
        try {
            DPAASExecutor executor;
            while((executor= getExecutor())!=null) {
                try {
                    return executor.getJobState(jobId);
                } catch (DPAASExecutorException e1) {
                    isAlive =false;
                }
            }
        }
        catch (DPAASHAExecutorException e)
        {
            throw new DPAASExecutorException(e.getExecutor(),e);
        }
        throw new  DPAASExecutorException(this,"Failed");
    }


    /**
     * @return the next executor
     * @throws DPAASHAExecutorException thrown when all the executors are failed.
     */
    private DPAASExecutor getExecutor() throws DPAASHAExecutorException {
        if(isAlive)return currentExecutor;
        failedExecutorQueue.add(executorQueue.poll());
        if(executorQueue.isEmpty())
        {
            executorQueue= failedExecutorQueue;
            currentExecutor=executorQueue.element();
            throw new DPAASHAExecutorException(this,"Failed with all the executors.");
        }
        currentExecutor=executorQueue.element();
        return currentExecutor;
    }
}
