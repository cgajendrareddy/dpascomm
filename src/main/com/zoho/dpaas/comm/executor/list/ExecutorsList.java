package com.zoho.dpaas.comm.executor.list;

import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import lombok.ToString;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

@ToString
public class ExecutorsList {
    Map<Integer,Executor> executors;

    public ExecutorsList(Map<Integer,Executor> executors)
    {
        this.executors=executors;
    }

    /**
     * Get Executor For JobId
     * @param id
     * @return
     */
    public Executor getExecutor(int id)
    {
        return executors.get(id);
    }

    /**
     * Get Executor For JobType
     * @param jobType
     * @return
     * @throws ExecutorException
     */
    public Executor getExecutor(String jobType) throws ExecutorException {
        Set<Executor> executorSet=getExecutors(jobType);
        for(Executor executor: executorSet)
        {
            if(executor.isEnabled() && executor.isRunning() && executor.isResourcesAvailableFortheJob(jobType))
            return executor;
        }
        throw new ExecutorException(null,"No Executors are ready to do the job "+jobType);//No I18N

    }

    /**
     * Get All Executors for JobType
     * @param jobType
     * @return
     */
    private Set<Executor> getExecutors(String jobType)
    {
       Set<Executor> toReturn=new TreeSet<>(new Comparator<Executor>(){
           @Override
           public int compare(Executor o1, Executor o2) {
               return Integer.compare(o1.getPriority(),o2.getPriority());
           }
    }) ;
       for(Executor executor:executors.values())
       {
           if(executor.getJobTypes().get(jobType)!=null)
           {
                toReturn.add(executor);

           }
       }
        return toReturn;
    }
}
