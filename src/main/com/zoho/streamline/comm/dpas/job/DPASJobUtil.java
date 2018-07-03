//$Id$
package com.zoho.streamline.comm.dpas.job;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by elam-4191 on 5/22/2017.
 *
 */
public class DPASJobUtil {
    public enum DPASJobState {
        INITIALIZED,
        SUBMITTED,
        INPROGRESS,
        ERROR,
        FAILED,
        SUCCESS,
        KILLED;
    }
    public enum DPASJobType {
        SAMPLE_TRANSFORMATION("sampletransformation"),//No I18N
        TRANSFORMATION("datasettransformation"),//No I18N
        GET_SAMPLE("sampleextract"),//No I18N
        DSAUDITSTATE_FILE("dsauditstatefile"),//No I18N
        RAWDSAUDIT_TRANSFORMATION("rawdsaudittransformation"),//No I18N
        SAMPLE_PREVIEW("samplepreview"),//No I18N
        ERROR_AUDIT("erroraudit"),//No I18N
        DETECT_ENCODING("detectencoding");//No I18N


        String jobType;
        DPASJobType(String jobType){
            this.jobType=jobType;
        }

        /**
         * Get DPASJobType for job type string
         * @param jobType
         * @return
         */
        public static DPASJobType getDPASJobType(String jobType){
            for(DPASJobType dpasJobType : DPASJobType.values()){
                if(jobType.equals(dpasJobType.jobType)){
                    return dpasJobType;
                }
            }
            return null;
        }

        @Override
        public String toString(){
            return jobType;
        }
    }
    public enum DPASJobOutputState {
        INPROGRESS,
        FAILED,
        KILLED,
        SUCCESS;
    }

    public enum ExecutorType {
        SPARKSJS,SPARKSDCLUSTER,SPARKLOCAL
    	/*
        SPARK("http://192.168.223.146:6066",sparkProperties),
        FLINK("",null);
        String serverURL;
        JSONObject params;
        ExecutorType(String serverURL, JSONObject params){
            this.serverURL=serverURL;
            this.params=params;
        }
        public String getServerURL(){
            return this.serverURL;
        }
        public JSONObject getParams(){
            return this.params;
        }
    */}
}
