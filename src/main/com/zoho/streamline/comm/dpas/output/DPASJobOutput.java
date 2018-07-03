//$Id$
package com.zoho.streamline.comm.dpas.output;


import com.zoho.streamline.comm.dpas.job.DPASJobUtil.ExecutorType;
import com.zoho.streamline.comm.dpas.job.DPASJobUtil.DPASJobOutputState;
import org.json.JSONObject;

/**
 * Created by elam-4191 on 5/19/2017.
 */
public class DPASJobOutput {
    String jobID;
    ExecutorType executorType;
    DPASJobOutputState jobOutputState;
    JSONObject additionalInfo=new JSONObject();
    /**
     * @param jobID
     * @param executorType
     * @param jobOutputState
     * @param additionalInfo
     */
    public DPASJobOutput(String jobID, ExecutorType executorType, DPASJobOutputState jobOutputState, JSONObject additionalInfo){
        this.jobID=jobID;
        this.executorType=executorType;
        this.jobOutputState=jobOutputState;
        this.additionalInfo=additionalInfo;
    }
    /**
     * @return
     */
    public String getJobID() {
        return jobID;
    }

    /**
     * @return
     */
    public ExecutorType getExecutorType() {
        return executorType;
    }

    /**
     * @return
     */
    public DPASJobOutputState getState() {
        return jobOutputState;
    }

    /**
     * @return
     */
    public JSONObject getAdditionalInfo() {
        return additionalInfo;
    }

}
