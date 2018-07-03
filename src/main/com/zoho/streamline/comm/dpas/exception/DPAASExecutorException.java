//$Id$
package com.zoho.streamline.comm.dpas.exception;

import com.zoho.dpaas.comm.executor.intrface.DPAASExecutor;

public class DPAASExecutorException extends Exception{
    DPAASExecutor executor;
    /**
     * @param message
     */
    public DPAASExecutorException(DPAASExecutor executor,String message){
        this(executor,message,null);
    }
    /**
     * @param message
     * @param e
     */
    public DPAASExecutorException(DPAASExecutor executor,String message, Throwable e){
        super(message,e);
        this.executor=executor;
    }
    /**
     * @param e
     */
    public DPAASExecutorException(DPAASExecutor executor,Throwable e){
        this(executor,null,e);
    }
}
