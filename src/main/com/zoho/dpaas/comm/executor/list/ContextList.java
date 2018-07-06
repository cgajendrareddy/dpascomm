package com.zoho.dpaas.comm.executor.list;

import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.job.JobType;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobInfo;

import java.util.*;


public class ContextList {
    private final JobList jobList;
    private final Map<String,List<String>> jobTypeContextMap;
    final List<String> contexts;
    public static final String jobTypeSeperator="@";
    public static final String INFO_STATUS_RUNNING = "RUNNING";
    public ContextList(List<String> contexts, List<SparkJobInfo> sparkJobInfo)
    {
        this.contexts=contexts;
        this.jobTypeContextMap=getJobTypeContextMap();
        this.jobList=new JobList(sparkJobInfo);
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
    public String getExistingAvailableContext(JobType jobType)
    {
        String availableContext=getAvailableContextFortheNewJob(jobType);
        if(availableContext!=null)
        {
            return availableContext;
        }
        return null;
    }
    public String getNewContext(JobType jobType) throws ExecutorException {
        int minPoolSize=jobType.getMinPool();
        int currentPoolSize=(jobTypeContextMap.containsKey(jobType.getJobType()))?jobTypeContextMap.get(jobType.getJobType()).size():0;
        if(currentPoolSize<minPoolSize)
        {
            return getNewContextName(jobType);
        }
        else{
            throw new ExecutorException(null,"No Contexts can further be created for the jobType"+jobType);
        }

    }
    private String getAvailableContextFortheNewJob(JobType jobType)
    {
        List<String> availableContexts=jobList.getAvailableContexstForJob(contexts,jobType);
        if(availableContexts!=null && !availableContexts.isEmpty())
            return availableContexts.get(0);
        return null;
    }

    private List<String> getContexts(JobType jobType) {

        return jobList.getAvailableContexstForJob(contexts,jobType);
    }

    private String getNewContextName(JobType jobType)
    {
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
