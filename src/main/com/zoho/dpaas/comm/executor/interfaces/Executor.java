//$Id$
package com.zoho.dpaas.comm.executor.interfaces;

import com.zoho.dpaas.comm.executor.conf.ExecutorConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.exception.HAExecutorException;
import com.zoho.dpaas.comm.executor.job.JobType;

import java.util.List;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.ExecutorType;
import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

public interface Executor {
    /**
     * Get Executor Id
     * @return
     */
    public int getId();

    /**
     * Returns true,if the executor is available
     * @return
     */
    public boolean isEnabled();

    /**
     * Get config JSON
     * @return
     */
    public ExecutorConfig getConf();

    /**
     * @return the priority of the executor
     * @throws ExecutorException
     */
    public int getPriority();

    /**
     * @return
     * @throws ExecutorException
     */
    public List<JobType> getJobTypes();
    /**
     * Get ExecutorType of Executor
     * @return
     */
    public ExecutorType getType();


    /**
     * To check whether there are executors available to submit a job.
     * @param jobType
     * @return
     */
    public boolean isResourcesAvailableFortheJob(String jobType) throws ExecutorException;

    /**
     * Submit a job to DPAAS via executor
     * @param appArgs
     * @return
     * @throws ExecutorException
     */
    public String submit(String... appArgs) throws ExecutorException;

    /**
     * Kill a submitted job
     * @param jobId
     * @return status of the job
     * @throws ExecutorException
     */
    public boolean killJob(String jobId) throws ExecutorException;

    /**
     * Poll for a submitted job
     * @param jobId
     * @return
     * @throws ExecutorException
     */
    public JobState getJobState(String jobId) throws ExecutorException;



}
