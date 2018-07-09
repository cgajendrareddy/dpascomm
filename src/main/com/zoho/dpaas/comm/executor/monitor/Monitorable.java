package com.zoho.dpaas.comm.executor.monitor;

import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.Executor;

public interface Monitorable {
    public void setIsRunning(boolean running);
    public void monitor() throws ExecutorException;
    public String getMonitorName();
}
