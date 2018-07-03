//$Id$
package com.zoho.streamline.comm.dpas.input;

import com.zoho.streamline.comm.dpas.exception.DPASJobException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created by elam-4191 on 5/19/2017.
 *
 * Specific to DPASJob interface
 */
public class DPASJobInput {
    JSONObject jsonObject=new JSONObject();
    /**
     * @param key
     * @param value
     * @throws DPASJobException
     */
    public void setParam(String key,String value) throws DPASJobException {
        try {
            this.jsonObject.put(key,value);
        } catch (JSONException e) {
            throw new DPASJobException(e);
        }
    }
    /**
     * @param key
     * @param value
     * @throws DPASJobException
     */
    public void setJsonObject(String key,JSONObject value) throws DPASJobException {
        try {
            this.jsonObject.put(key,value);
        } catch (JSONException e) {
            throw new DPASJobException(e);
        }
    }
    /**
     * @param key
     * @param value
     * @throws DPASJobException
     */
    public void setJsonArray(String key,JSONArray value) throws DPASJobException {
        try {
            this.jsonObject.put(key,value);
        } catch (JSONException e) {
            throw new DPASJobException(e);
        }
    }
    /**
     * @param key
     * @return
     * @throws DPASJobException
     */
    public String getParam(String key) throws DPASJobException {
        try {
            return this.jsonObject.getString(key);
        } catch (JSONException e) {
            throw new DPASJobException(e);
        }
    }
    /**
     * @param key
     * @return
     * @throws DPASJobException
     */
    public JSONObject getJsonObject(String key) throws DPASJobException {
        try {
            return this.jsonObject.getJSONObject(key);
        } catch (JSONException e) {
            throw new DPASJobException(e);
        }
    }
    /**
     * @param key
     * @return
     * @throws DPASJobException
     */
    public JSONArray getJsonArray(String key) throws DPASJobException {
        try {
            return this.jsonObject.getJSONArray(key);
        } catch (JSONException e) {
            throw new DPASJobException(e);
        }
    }
    /**
     * @return
     * @throws DPASJobException
     * @throws JSONException
     */
    public JSONObject getInputJson() throws DPASJobException, JSONException
    {
        return this.jsonObject;
    }
}
