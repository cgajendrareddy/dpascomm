//$Id$
package com.zoho.streamline.comm.dpas.intrface;

import com.zoho.streamline.comm.dpas.exception.DPASJobException;
import com.zoho.streamline.comm.dpas.input.DPASJobInput;
import com.zoho.streamline.comm.dpas.job.DPASJobUtil;
import com.zoho.streamline.comm.dpas.output.DPASJobOutput;
import org.json.JSONObject;

import java.util.Map;

public interface JobExecutor {
    /**
     * Get Executor Id
     * @return
     */
    public int getId() throws DPASJobException;

    /**
     * Returns true,if the executor is available
     * @return
     */
    public boolean isAvailable();

    /**
     * Returns true,if the executor is available
     * @return
     */
    public boolean isEnabled();

    /**
     * Get config JSON
     * @return
     */
    public JSONObject getConfigJSON();

    /**
     * Get ExecutorType of JobExecutor
     * @return
     */
    public DPASJobUtil.ExecutorType getExecutorType();

    /**
     * Submit a job to DPAAS via executor
     * @param jobInput
     * @return
     * @throws DPASJobException
     */
    public Map<String,String> submit(DPASJobInput jobInput) throws DPASJobException;

    /**
     * Kill a submitted job
     * @param dpasJob
     * @return
     * @throws DPASJobException
     */
    public boolean kill(DPASJob dpasJob) throws DPASJobException;

    /**
     * Poll for a submitted job
     * @param dpasJob
     * @return
     * @throws DPASJobException
     */
    public DPASJobOutput poll(DPASJob dpasJob) throws DPASJobException;

}
