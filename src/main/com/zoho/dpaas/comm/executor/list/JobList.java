package com.zoho.dpaas.comm.executor.list;

import com.zoho.dpaas.comm.executor.constants.ExecutorConstants;
import com.zoho.dpaas.comm.executor.job.JobType;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobList extends ExecutorConstants {
    private final List<SparkJobInfo> jobs;
    private final Map<String,Map<String,List<SparkJobInfo>>> jobsMap;
    public JobList(List<SparkJobInfo> sparkJobInfo)
    {
        this.jobs=sparkJobInfo;
        this.jobsMap=getContextVsJobsMap();
    }

    /**
     * Get Map of Context and SparkJob
     * @return
     */
    private Map<String,Map<String,List<SparkJobInfo>>>  getContextVsJobsMap()
    {
        Map<String,Map<String,List<SparkJobInfo>>> toReturn=new HashMap<>();
        if(jobs!=null) {
            for (SparkJobInfo jobInfo : jobs) {
                if(jobInfo!=null) {
                    String contextName = jobInfo.getContext();
                    String status = jobInfo.getStatus();
                    if(contextName!=null && status!=null && !contextName.isEmpty() && !status.isEmpty()) {
                        Map<String, List<SparkJobInfo>> jobsMap = toReturn.get(contextName);
                        if (jobsMap == null) {
                            jobsMap = new HashMap<>();
                            toReturn.put(contextName, jobsMap);
                        }
                        List<SparkJobInfo> jobsList = jobsMap.get(status);
                        if (jobsList == null) {
                            jobsList = new ArrayList<>();
                            jobsMap.put(status, jobsList);
                        }
                        jobsList.add(jobInfo);
                    }
                }
            }
        }
       return toReturn;
    }

    /**
     * Get Avaliable Contexts for The JobType
     * @param contexts
     * @param jobTypeObj
     * @return
     */
    public List<String> getAvailableContexstForJob(List<String> contexts, JobType jobTypeObj)
    {
        List<String> toReturn=new ArrayList<>();
        if(jobTypeObj!=null && contexts!=null && !contexts.isEmpty()) {
            String jobtype = jobTypeObj.getJobType();
            for (String context : contexts) {
                if (context != null && context.startsWith(jobtype)) {
                    List<SparkJobInfo> runningJobs=getJobsOfContext(context,INFO_STATUS_RUNNING);
                    if(runningJobs==null || runningJobs.isEmpty())
                    {
                        toReturn.add(context);
                    }
                }
            }
        }

        return toReturn;
    }

    /**
     * Get Jobs with specified context name
     * @param contextName
     * @param jobStatus
     * @return
     */
    public List<SparkJobInfo> getJobsOfContext(String contextName,String jobStatus)
    {
        List<SparkJobInfo> toReturn=null;
        if(contextName!=null && jobStatus!=null && !contextName.isEmpty() && !jobStatus.isEmpty() && jobsMap.containsKey(contextName))
        {
            Map<String,List<SparkJobInfo>> statusMap=jobsMap.get(contextName);
            if(statusMap!=null && statusMap.containsKey(jobStatus))
            {
                toReturn=statusMap.get(jobStatus);
            }
        }
        return toReturn;
    }

}
