package com.zoho.dpaas.comm.executor;

import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@ToString
public class ExecutorsList {
    Map<Integer,Executor> executors;
    public ExecutorsList(Map<Integer,Executor> executors)
    {
        this.executors=executors;
    }
    public Executor getExecutor(int id)
    {
        return executors.get(id);
    }

    public List<Executor> getExecutorListFor(String jobType) throws ExecutorConfigException,ExecutorException
    {
        return null;
    }
}
