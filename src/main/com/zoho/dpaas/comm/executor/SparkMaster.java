package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ywilkof.sparkrestclient.*;
import com.github.ywilkof.sparkrestclient.interfaces.JobStatusRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.KillJobRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.SparkPropertiesSpecification;
import com.zoho.dpaas.comm.executor.conf.SparkClusterConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractExecutor;
import com.zoho.dpaas.comm.executor.job.JobType;
import com.zoho.dpaas.comm.executor.list.ContextList;
import com.zoho.dpaas.comm.executor.monitor.ExecutorMonitor;
import com.zoho.dpaas.comm.executor.monitor.Monitorable;
import com.zoho.dpaas.comm.util.DPAASCommUtil;
import org.apache.http.client.HttpClient;
import org.json.JSONObject;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobInfo;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkMasterJobInfo;

import java.io.IOException;
import java.util.*;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

public class SparkMaster extends AbstractExecutor implements Monitorable {

    private ContextList contextList;
    SparkRestClient client;
    boolean isRunning;

    /**
     * @param sparkMasterConfig
     * @throws ExecutorConfigException
     */
    public SparkMaster(JSONObject sparkMasterConfig) throws ExecutorConfigException {
        super(getSparkExecutorConf(sparkMasterConfig));

        SparkClusterConfig conf = (SparkClusterConfig) getConf();
        HttpClient httpClient = DPAASCommUtil.getHttpClient(30000);
        client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).httpClient(httpClient).masterHost(conf.getHost()).masterPort(conf.getPort()).environmentVariables(conf.getEnvironmentVariables()).build();
        try {
            monitor();
        } catch (ExecutorException e) {
            e.printStackTrace();
        }
        finally {
            new ExecutorMonitor(this).start();
        }


    }

    /**
     * @param sparkMasterConfig
     * @return
     * @throws ExecutorConfigException
     */
    static SparkClusterConfig getSparkExecutorConf(JSONObject sparkMasterConfig) throws ExecutorConfigException {
        try {
            return new ObjectMapper().readValue(sparkMasterConfig.toString(),SparkClusterConfig.class);
        } catch (IOException e){
            throw new ExecutorConfigException("Unable to initialize SparkCluster Conf",e);//No I18N
        }
    }

    @Override
    public boolean isResourcesAvailableFortheJob(String jobType) throws ExecutorException {
        SparkClusterDetailsResponse clusterDetails=getSparkClusterDetails();
        int avaialbleCores=clusterDetails.getCores()-clusterDetails.getCoresused();
        if(getConf().getJobTypes().get(jobType) == null){
            throw new ExecutorException(this,"Job Type "+jobType+" not found in the executors supported JobTypes");//No I18N
        }
        int requiredCores = getConf().getJobTypes().get(jobType).getCores();

        return requiredCores<avaialbleCores;
    }

    /**
     * Get Context for specified JobType
     * @param jobtype
     * @return
     * @throws ExecutorException
     */
    private String getContextForTheJob(JobType jobtype) throws ExecutorException {
        try {
            return contextList.getNewContext(jobtype);
        }catch (ExecutorException e)
        {
            throw new ExecutorException(this,e);
        }
    }

    @Override
    public String submit(String jobType, String[] jobArgs) throws ExecutorException {
        SparkClusterConfig conf = (SparkClusterConfig) getConf();
        JobType jobTypeObj =getConf().getJobTypes().get(jobType);
        if(jobTypeObj == null){
            throw new ExecutorException(this,"Unsupported Job Type "+jobType);//No I18N
        }
        if(!isResourcesAvailableFortheJob(jobType)){
            throw new ExecutorException(this,"Insufficient Resources to execute the JobType :"+jobType);//No I18N
        }
        JobSubmitRequestSpecificationImpl jobSubmit = new JobSubmitRequestSpecificationImpl(client);
        jobSubmit.appName(getContextForTheJob(jobTypeObj));
        jobSubmit.appResource(conf.getAppResource());
        if(conf.getJars() !=null && !conf.getJars().isEmpty()){
            jobSubmit.usingJars(conf.getJars());
        }
        jobSubmit.mainClass(jobTypeObj.getClassPath()!=null?jobTypeObj.getClassPath():conf.getClassPath());
        jobSubmit.appArgs(Arrays.asList(jobArgs));
        SparkPropertiesSpecification sparkPropertiesSpecification = jobSubmit.withProperties();
        //Insert ExecutorParams for executor creation
        Map<String,String> executorParams = new HashMap<>(jobTypeObj.getParamsForExecutorCreation(conf.getClusterMode()));
        for (Map.Entry<String, String> entry : executorParams.entrySet()) {
            sparkPropertiesSpecification.put(entry.getKey(), entry.getValue());
        }
        //Insert Config params from the executor configurations
        Map<String,String> config = conf.getConfig();
        for (Map.Entry<String, String> entry : config.entrySet()) {
            sparkPropertiesSpecification.put(entry.getKey(), entry.getValue());
        }

        try {

            return jobSubmit.submit();
        } catch (FailedSparkRequestException e) {
            throw new ExecutorException(this,"JOB Failed. Message : "+e.getMessage(),e);//No I18N
        }
    }

    @Override
    public boolean killJob(String jobId) throws ExecutorException {
        //TODO check for multiple master url's and do the same
        KillJobRequestSpecification killJobRequestSpecification = client.killJob();
        try {
            return killJobRequestSpecification.withSubmissionId(jobId);
        }catch (FailedSparkRequestException e){
            throw new ExecutorException(this,"Kill Job Request Failed. Message:  "+e.getMessage(),e);//No I18N
        }
    }

    @Override
    public JobState getJobState(String jobId) throws ExecutorException {
        JobStatusRequestSpecification jobStatusRequestSpecification = client.checkJobStatus();
        try{
            return JobState.valueOf(jobStatusRequestSpecification.withSubmissionId(jobId).name());
        } catch (FailedSparkRequestException e) {
            throw new ExecutorException(this,"Unable to get Job Status. Message : "+e.getMessage(),e);//No I18N
        }
    }

    @Override
    public boolean isRunning(){
       return isRunning;
    }

    /**
     * Get Spark Cluster Details
     * @return
     * @throws ExecutorException
     */
    private SparkClusterDetailsResponse getSparkClusterDetails() throws ExecutorException {
        try {
            HttpClient httpClient = DPAASCommUtil.getHttpClient(30000);
            SparkRestClient confClient = SparkRestClient.builder().sparkVersion(client.getSparkVersion()).httpClient(httpClient).masterPort(((SparkClusterConfig)getConf()).getWebUIPort()).masterHost(client.getMasterHost()).build();
            return new SparkClusterDetailsSpecificationImpl(confClient).getSparkClusterDetails();
        }
        catch(Exception e)
        {
            throw new ExecutorException(this,e);
        }
    }

    @Override
    public void setIsRunning(boolean running) {
        this.isRunning=running;
    }

    @Override
    public void monitor() throws ExecutorException {
        SparkClusterDetailsResponse clusterDetails = null;
        clusterDetails = getSparkClusterDetails();

        List<Context> contexts = clusterDetails.getActiveapps();
        List<String> contextNames = new ArrayList<>();
        List<SparkJobInfo> sparkJobInfos = new ArrayList<>();
        for (Context context : contexts) {
            if (context != null) {
                contextNames.add(context.getName());
            }
            SparkMasterJobInfo jobInfo = new SparkMasterJobInfo();
            jobInfo.setContext(context.getName());
            jobInfo.setJobId(context.getId());
            jobInfo.setStartTime(context.getStarttime());
            jobInfo.setStatus(context.getState());
            jobInfo.setDuration(String.valueOf(context.getDuration().longValue()));
            sparkJobInfos.add(jobInfo.getSparkJobInfo());
        }
        contextList = new ContextList(contextNames, sparkJobInfos);
    }

    @Override
    public String getMonitorName() {
        return this.getType()+getConf().getName();
    }
}
