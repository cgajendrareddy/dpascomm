//$Id$
package com.zoho.streamline.comm.dpas.exception;

/**
 * Created by elam-4191 on 5/19/2017.
 *
 * DPASJobException specific for DPASJOB interface and it's implementation
 */
public class DPASJobException extends Exception{
    /**
     * @param message
     */
    public DPASJobException(String message){
        super(message);
    }
    /**
     * @param message
     * @param e
     */
    public DPASJobException(String message,Throwable e){
        super(message,e);
    }
    /**
     * @param e
     */
    public DPASJobException(Throwable e){
        super(e);
    }

}
