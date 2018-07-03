//$Id$
package com.zoho.streamline.comm.dpas.intrface;

import com.zoho.streamline.comm.dpas.job.DPASJobUtil.DPASJobState;

/**
 * Created by elam-4191 on 5/19/2017.
 *
 * DPASJob interface implemented to create job Object to interact with DPAS layer(SPARK,..)
 *
 * APIs:
 * 		1.getID -> get JOBID/APPID that is created by DPAS
 * 		2.getExecutorType -> SPARK/other type
 * 		3.getType -> Job type (Profiling,transformation,..)
 * 		4.getState -> SUBMITTED,RUNNNING,..
 * 		5.getRecentOutput -> get currentOutput without polling
 * 		6.getOutput -> get Output,if not available poll the output from DPAS
 * 		7.Kill,Submit -> DPAS Related Apis to submit and kill job in DPAS respectively. 
 *
 */
public interface DPASJob {
    String getID();
    void setID(String id);
    DPASJobState getState();
    void setState(DPASJobState dpasJobState);
}
 