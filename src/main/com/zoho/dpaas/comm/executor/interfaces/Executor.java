//$Id$
package com.zoho.dpaas.comm.executor.interfaces;

import com.zoho.dpaas.comm.executor.conf.ExecutorConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;

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
     * Get ExecutorType of Executor
     * @return
     */
    public ExecutorType getType();

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