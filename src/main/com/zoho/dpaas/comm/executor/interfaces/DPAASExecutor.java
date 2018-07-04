//$Id$
package com.zoho.dpaas.comm.executor.interfaces;

import com.zoho.dpaas.comm.executor.conf.DPAASExecutorConf;
import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.ExecutorType;
import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

public interface DPAASExecutor {
    /**
     * Get DPAASExecutor Id
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
    public DPAASExecutorConf getConf();

    /**
     * Get ExecutorType of DPAASExecutor
     * @return
     */
    public ExecutorType getType();

    /**
     * Submit a job to DPAAS via executor
     * @param appArgs
     * @return
     * @throws DPAASExecutorException
     */
    public String submit(String... appArgs) throws DPAASExecutorException;

    /**
     * Kill a submitted job
     * @param jobId
     * @return status of the job
     * @throws DPAASExecutorException
     */
    public boolean killJob(String jobId) throws DPAASExecutorException;

    /**
     * Poll for a submitted job
     * @param jobId
     * @return
     * @throws DPAASExecutorException
     */
    public JobState getJobState(String jobId) throws DPAASExecutorException;

}
