package com.zoho.dpaas.comm.executor.exception;

import com.zoho.dpaas.comm.executor.interfaces.Executor;
import lombok.Getter;

public class ExecutorException extends Exception{
    @Getter
    Executor executor;
    /**
     * @param message
     */
    public ExecutorException(Executor executor, String message){
        this(executor,message,null);
    }
    /**
     * @param message
     * @param e
     */
    public ExecutorException(Executor executor, String message, Throwable e){
        super(message,e);
        this.executor=executor;
    }
    /**
     * @param e
     */
    public ExecutorException(Executor executor, Throwable e){
        this(executor,null,e);
    }
}
