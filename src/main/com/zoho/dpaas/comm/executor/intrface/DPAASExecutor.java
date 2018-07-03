//$Id$
package com.zoho.dpaas.comm.executor.intrface;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.*;

import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;
import org.json.JSONObject;

import javax.print.attribute.standard.JobState;

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
    public JSONObject getConf();

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
