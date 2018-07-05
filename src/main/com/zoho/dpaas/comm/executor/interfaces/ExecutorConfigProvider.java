package com.zoho.dpaas.comm.executor.interfaces;

import com.zoho.dpaas.comm.executor.conf.ExecutorConfig;
import org.json.JSONObject;

public interface ExecutorConfigProvider {

    /**
     * @param id
     * @return executor conf object for the executor id.
     */
    public JSONObject getExecutorConfig(int id);

    public JSONObject getExecutorConfigs();

}
