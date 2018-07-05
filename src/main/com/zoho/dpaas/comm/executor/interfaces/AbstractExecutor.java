package com.zoho.dpaas.comm.executor.interfaces;

import com.zoho.dpaas.comm.executor.conf.ExecutorConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;
import lombok.NonNull;


import static com.zoho.dpaas.comm.util.DPAASCommUtil.ExecutorType;


public abstract class AbstractExecutor implements Executor {
    @NonNull
    ExecutorConfig executorConf;


    public AbstractExecutor(ExecutorConfig executorConf){
        this.executorConf = executorConf;
    }

    @Override
    public int getId() {
        return this.executorConf.getId();
    }

    @Override
    public boolean isEnabled() {
        return !executorConf.getDisabled();
    }

    @Override
    public ExecutorConfig getConf() {
        return executorConf;
    }

    @Override
    public ExecutorType getType() { return executorConf.getType(); }

    @Override
    public int getPriority(){
        return executorConf.getPriority();
    }
    @Override
    public abstract String submit(String... appArgs) throws ExecutorException;

    @Override
    public abstract boolean killJob(String jobId) throws ExecutorException;

    @Override
    public abstract JobState getJobState(String jobId) throws ExecutorException;
}
