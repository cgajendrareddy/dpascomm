//$Id$
package com.zoho.dpaas.comm.executor.exception;

import com.zoho.dpaas.comm.executor.interfaces.DPAASExecutor;
import lombok.Getter;

public class DPAASExecutorException extends Exception{
    @Getter
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
