package com.zoho.dpaas.comm.executor.interfaces;

import com.zoho.dpaas.comm.executor.conf.DPAASExecutorConf;
import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;
import lombok.NonNull;

import javax.print.attribute.standard.JobState;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.ExecutorType;


public abstract class AbstractDPAASExecutor implements DPAASExecutor {
    @NonNull
    DPAASExecutorConf executorConf;


    public AbstractDPAASExecutor(DPAASExecutorConf executorConf){
        this.executorConf = executorConf;
    }

    @Override
    public int getId() {
        return this.executorConf.getId();
    }

    @Override
    public boolean isEnabled() {
        return this.executorConf.getDisabled();
    }

    @Override
    public DPAASExecutorConf getConf() {
        return this.executorConf;
    }

    @Override
    public ExecutorType getType() { return this.executorConf.getType(); }

    @Override
    public abstract String submit(String... appArgs) throws DPAASExecutorException ;

    @Override
    public abstract boolean killJob(String jobId) throws DPAASExecutorException ;

    @Override
    public abstract JobState getJobState(String jobId) throws DPAASExecutorException ;
}
