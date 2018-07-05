package com.zoho.dpaas.comm.executor;

import com.zoho.dpaas.comm.executor.job.JobType;

import java.util.*;

public class ContextList {

    private Map<String,List<String>> jobTypeContextMap;
    List<String> contexts;
    public static final String jobTypeSeperator="@";
    public ContextList(List<String> contexts)
    {
        this.contexts=contexts;
        this.jobTypeContextMap=getJobTypeContextMap();
    }

    private Map<String,List<String>> getJobTypeContextMap()
    {
        Map<String,List<String>> jobTypeContextMap=new HashMap<>();
        for(String contextName : contexts)
        {
            String jobType=(contextName.split(jobTypeSeperator))[0];
            List<String> contexts=jobTypeContextMap.get(jobType);
            if(contexts==null)
            {
                contexts=new ArrayList<>();
                jobTypeContextMap.put(jobType,contexts);
            }
            contexts.add(contextName);
        }
        return jobTypeContextMap;
    }

    public String getNewContextName(JobType jobType)
    {
        int minPoolSize=jobType.getMinPool();
        int currentPoolSize=(jobTypeContextMap.containsKey(jobType.getJobType()))?jobTypeContextMap.get(jobType.getJobType()).size():0;
        if(currentPoolSize>=minPoolSize)
        {
            throw new RuntimeException("No Contexts can further be created for the jobType"+jobType);
        }
        return jobType+jobTypeSeperator+getNextContextIndex(jobType);
    }
    private int getNextContextIndex(JobType jobType)
    {
        int toReturn=1;
        if(jobTypeContextMap.containsKey(jobType.getJobType()))
        {
            List<String> contexts=jobTypeContextMap.get(jobType.getJobType());
            for(String contextName:contexts )
            {
                int contextIndex=Integer.parseInt(contextName.split(jobTypeSeperator)[1]);
                if(toReturn<contextIndex)
                {
                    toReturn=contextIndex;
                }
            }
        }
        return toReturn;
    }

}
