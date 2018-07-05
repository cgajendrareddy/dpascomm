package com.zoho.dpaas.comm.executor.exception;

public class ExecutorConfigException  extends Exception {
    /**
     * @param message
     */
    public ExecutorConfigException(String message){
        this(message,null);
    }
    /**
     * @param message
     * @param e
     */
    public ExecutorConfigException(String message, Throwable e){
        super(message,e);
    }
    /**
     * @param e
     */
    public ExecutorConfigException(Throwable e){
        this(e.getMessage(),e);
    }
}
