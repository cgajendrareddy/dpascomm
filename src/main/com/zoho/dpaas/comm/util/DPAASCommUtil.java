package com.zoho.dpaas.comm.util;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpParams;

/**
 * Created by elam-4191 on 5/22/2017.
 *
 */
public class DPAASCommUtil {

    public enum JobState {
        SUBMITTED("INPROGRESS"),//No I18N
        RUNNING("INPROGRESS"),//No I18N
        FINISHED("SUCCESS"),//No I18N
        RELAUNCHING("INPROGRESS"),//No I18N
        UNKNOWN("FAILED"),//No I18N
        KILLED("KILLED"),//No I18N
        FAILED("FAILED"),//No I18N
        ERROR("FAILED"),//No I18N
        QUEUED("INPROGRESS"),//No I18N
        RETRYING("INPROGRESS"),//No I18N
        NOT_FOUND("FAILED");//No I18N

        JobState(String failed) {
        }
    }

    /**
     * Get Http Client
     * @return
     */
    public static org.apache.http.client.HttpClient getHttpClient(int timeout){
        org.apache.http.client.HttpClient httpClient = new DefaultHttpClient();
        HttpParams httpParams = httpClient.getParams().setParameter("http.connection.timeout", new Integer(timeout));//No I18N
        ((DefaultHttpClient) httpClient).setParams(httpParams);
        return httpClient;
    }

    /**
     * Set TimeOut Param for client
     * @param client
     * @param timeout
     */
    public static void addTimeOutParameter(org.apache.http.client.HttpClient client,int timeout){
        HttpParams httpParams = client.getParams().setParameter("http.connection.timeout", new Integer(timeout));//No I18N
        ((DefaultHttpClient) client).setParams(httpParams);
    }
}
