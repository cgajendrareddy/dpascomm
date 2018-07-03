package com.zoho.dpaas.comm.executor.exception;

import com.zoho.dpaas.comm.executor.intrface.DPAASExecutor;
import lombok.Getter;

public class DPAASHAExecutorException extends Exception{
    @Getter
    DPAASExecutor executor;
    /**
     * @param message
     */
    public DPAASHAExecutorException(DPAASExecutor executor,String message){
        this(executor,message,null);
    }
    /**
     * @param message
     * @param e
     */
    public DPAASHAExecutorException(DPAASExecutor executor,String message, Throwable e){
        super(message,e);
        this.executor=executor;
    }
    /**
     * @param e
     */
    public DPAASHAExecutorException(DPAASExecutor executor,Throwable e){
        this(executor,null,e);
    }
}