//$Id$
package com.zoho.streamline.comm.dpas.job;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by elam-4191 on 5/22/2017.
 *
 */
public class Util {

    public enum JobState {
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
