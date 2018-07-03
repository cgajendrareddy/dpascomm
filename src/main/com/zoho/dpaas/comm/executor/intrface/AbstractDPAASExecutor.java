package com.zoho.dpaas.comm.executor.intrface;

import com.zoho.dpaas.comm.util.DPAASCommUtil;
import com.zoho.streamline.comm.dpas.exception.DPAASExecutorException;
import org.json.JSONObject;

import javax.print.attribute.standard.JobState;

public abstract class AbstractDPAASExecutor implements DPAASExecutor {
    @Override
    public int getId() {
        return 0;
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

    @Override
    public JSONObject getConf() {
        return null;
    }

    @Override
    public DPAASCommUtil.ExecutorType getType() {
        return null;
    }

    @Override
    public String submit(String... appArgs) throws DPAASExecutorException {
        return null;
    }

    @Override
    public boolean killJob(String jobId) throws DPAASExecutorException {
        return false;
    }

    @Override
    public JobState getJobState(String jobId) throws DPAASExecutorException {
        return null;
    }
}
