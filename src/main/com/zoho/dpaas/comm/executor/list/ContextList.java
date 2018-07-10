package com.zoho.dpaas.comm.executor.list;

import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.job.JobType;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.zoho.dpaas.comm.executor.constants.ExecutorConstants.JOBTYPESEPERATOR;


public class ContextList {
    private final JobList jobList;
    private final Map<String,List<String>> jobTypeContextMap;
    final List<String> contexts;
    public ContextList(List<String> contexts, List<SparkJobInfo> sparkJobInfo)
    {
        this.contexts=contexts;
        this.jobTypeContextMap=getJobTypeContextMap();
        this.jobList=new JobList(sparkJobInfo);
    }

    /**
     * Get Map of JobType and Contexts
     * @return
     */
    private Map<String,List<String>> getJobTypeContextMap()
    {
        Map<String,List<String>> jobTypeContextMap=new HashMap<>();
        for(String contextName : contexts)
        {
            String jobType=(contextName.split(JOBTYPESEPERATOR))[0];
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

    /**
     * Get Existing Available Contexts for the JobType
     * @param jobType
     * @return
     */
    public String getExistingAvailableContext(JobType jobType)
    {
        String availableContext=getAvailableContextFortheNewJob(jobType);
        if(availableContext!=null)
        {
            return availableContext;
        }
        return null;
    }

    /**
     * Get New Context Name
     * @param jobType
     * @return
     * @throws ExecutorException
     */
    public String getNewContext(JobType jobType) throws ExecutorException {
        int maxPoolSize=jobType.getMaxPool();
        int currentPoolSize=(jobTypeContextMap.containsKey(jobType.getJobType()))?jobTypeContextMap.get(jobType.getJobType()).size():0;
        if(currentPoolSize<maxPoolSize)
        {
            return getNewContextName(jobType);
        }
        else{
            throw new ExecutorException(null,"No Contexts can further be created for the jobType"+jobType);//No I18N
        }

    }

    /**
     * Get Avaliable Contexts for the Job
     * @param jobType
     * @return
     */
    private String getAvailableContextFortheNewJob(JobType jobType)
    {
        List<String> availableContexts=jobList.getAvailableContexstForJob(contexts,jobType);
        if(availableContexts!=null && !availableContexts.isEmpty())
            return availableContexts.get(0);
        return null;
    }

    /**
     * Get Contexts
     * @param jobType
     * @return
     */
    private List<String> getContexts(JobType jobType) {

        return jobList.getAvailableContexstForJob(contexts,jobType);
    }

    /**
     * Get New Context Name for JobType
     * @param jobType
     * @return
     */
    private String getNewContextName(JobType jobType)
    {
        return jobType.getJobType()+ JOBTYPESEPERATOR +getNextContextIndex(jobType);
    }

    /**
     * Get New Context Index
     * @param jobType
     * @return
     */
    private int getNextContextIndex(JobType jobType)
    {
        int toReturn=1;
        if(jobTypeContextMap.containsKey(jobType.getJobType()))
        {
            List<String> contexts=jobTypeContextMap.get(jobType.getJobType());
            for(String contextName:contexts )
            {
                int contextIndex=Integer.parseInt(contextName.split(JOBTYPESEPERATOR)[1]);
                if(toReturn<contextIndex)
                {
                    toReturn=contextIndex;
                }

            }
        }
        return toReturn;
    }

}
