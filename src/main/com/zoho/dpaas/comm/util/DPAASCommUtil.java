//$Id$
package com.zoho.dpaas.comm.util;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpParams;

/**
 * Created by elam-4191 on 5/22/2017.
 *
 */
public class DPAASCommUtil {

    public enum JobState {
        SUBMITTED("INPROGRESS"),
        RUNNING("INPROGRESS"),
        FINISHED("SUCCESS"),
        RELAUNCHING("INPROGRESS"),
        UNKNOWN("FAILED"),
        KILLED("KILLED"),
        FAILED("FAILED"),
        ERROR("FAILED"),
        QUEUED("INPROGRESS"),
        RETRYING("INPROGRESS"),
        NOT_FOUND("FAILED");

        JobState(String failed) {
        }
    }

    public enum ExecutorType {
        SPARK_SJS, SPARK_CLUSTER, LOCAL_SPARK,SPARK_HA
    }

    /**
     * Get Http Client
     * @return
     */
    public static org.apache.http.client.HttpClient getHttpClient(int timeout){
        org.apache.http.client.HttpClient httpClient = new DefaultHttpClient();
        HttpParams httpParams = httpClient.getParams().setParameter("http.connection.timeout", new Integer(timeout));
        ((DefaultHttpClient) httpClient).setParams(httpParams);
        return httpClient;
    }

    /**
     * Set TimeOut Param for client
     * @param client
     * @param timeout
     */
    public static void addTimeOutParameter(org.apache.http.client.HttpClient client,int timeout){
        HttpParams httpParams = client.getParams().setParameter("http.connection.timeout", new Integer(timeout));
        ((DefaultHttpClient) client).setParams(httpParams);
    }
}
