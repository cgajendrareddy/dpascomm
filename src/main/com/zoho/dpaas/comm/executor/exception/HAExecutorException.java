package com.zoho.dpaas.comm.executor.exception;

import com.zoho.dpaas.comm.executor.HAExecutor;
import lombok.Getter;

public class HAExecutorException extends Exception{
    @Getter
    HAExecutor executor;
    /**
     * @param message
     */
    public HAExecutorException(HAExecutor executor, String message){
        this(executor,message,null);
    }
    /**
     * @param message
     * @param e
     */
    public HAExecutorException(HAExecutor executor, String message, Throwable e){
        super(message,e);
        this.executor=executor;
    }
    /**
     * @param e
     */
    public HAExecutorException(HAExecutor executor, Throwable e){
        this(executor,null,e);
    }
}