package com.zoho.dpaas.comm.executor;

import com.zoho.dpaas.comm.executor.interfaces.Executor;
import lombok.ToString;

import java.util.List;
import java.util.Map;
import static java.util.stream.Collectors.*;

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

    public List<Executor> getExecutorListFor(String jobType)
    {
        return null;
    }
}
