//$Id$
package com.zoho.streamline.comm.dpas.intrface;

import com.zoho.streamline.comm.dpas.exception.DPASJobException;
import com.zoho.streamline.comm.dpas.job.Util;
import org.json.JSONObject;

import javax.print.attribute.standard.JobState;

public interface Executor {
    /**
     * Get Executor Id
     * @return
     */
    public int getId() throws DPASJobException;

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
     * Get ExecutorType of Executor
     * @return
     */
    public Util.ExecutorType getType();

    /**
     * Submit a job to DPAAS via executor
     * @param appArgs
     * @return
     * @throws DPASJobException
     */
    public String submit(String... appArgs) throws DPASJobException;

    /**
     * Kill a submitted job
     * @param jobId
     * @return status of the job
     * @throws DPASJobException
     */
    public boolean killJob(String jobId) throws DPASJobException;

    /**
     * Poll for a submitted job
     * @param jobId
     * @return
     * @throws DPASJobException
     */
    public JobState getJobState(String jobId) throws DPASJobException;

}
