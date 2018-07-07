package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ywilkof.sparkrestclient.*;
import com.github.ywilkof.sparkrestclient.interfaces.JobStatusRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.KillJobRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.SparkPropertiesSpecification;
import com.zoho.dpaas.comm.executor.conf.SparkClusterConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.exception.HAExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractExecutor;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import com.zoho.dpaas.comm.executor.job.JobType;
import com.zoho.dpaas.comm.util.DPAASCommUtil;
import org.apache.http.client.HttpClient;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

public class SparkMaster extends AbstractExecutor {

    SparkRestClient client;

    /**
     * @param sparkMasterConfig
     * @throws ExecutorConfigException
     */
    public SparkMaster(JSONObject sparkMasterConfig) throws ExecutorConfigException {
        super(getSparkExecutorConf(sparkMasterConfig));
        SparkClusterConfig conf = (SparkClusterConfig) getConf();
        HttpClient httpClient = (HttpClient) DPAASCommUtil.getHttpClient(15000);
        client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).httpClient(httpClient).masterHost(conf.getHost()).masterPort(conf.getPort()).environmentVariables(conf.getEnvironmentVariables()).build();
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
            throw new ExecutorConfigException("Unable to initialize SparkCluster Conf",e);
        }
    }

    @Override
    public boolean isResourcesAvailableFortheJob(String jobType) throws ExecutorException {
        SparkClusterDetailsResponse clusterDetails=getSparkClusterDetails();
        int avaialbleCores=clusterDetails.getCores()-clusterDetails.getCoresused();
        if(getConf().getJobTypes().get(jobType) == null){
            throw new ExecutorException(this,"Job Type "+jobType+" not found in the executors supported JobTypes");
        }
        int requiredCores = getConf().getJobTypes().get(jobType).getCores();

        return requiredCores<avaialbleCores;
    }

    @Override
    public String submit(String jobType, String[] jobArgs) throws ExecutorException {
        SparkClusterConfig conf = (SparkClusterConfig) getConf();
        JobType jobTypeObj =getConf().getJobTypes().get(jobType);
        if(jobTypeObj == null){
            throw new ExecutorException(this,"Unsupported Job Type "+jobType);
        }
        if(!isResourcesAvailableFortheJob(jobType)){
            throw new ExecutorException(this,"Insufficient Resources to execute the JobType :"+jobType);
        }
        JobSubmitRequestSpecificationImpl jobSubmit = new JobSubmitRequestSpecificationImpl(client);
        jobSubmit.appName(conf.getAppName());
        jobSubmit.appResource(conf.getAppResource());
        jobSubmit.mainClass(conf.getMainClass());
        jobSubmit.appArgs(Arrays.asList(jobArgs));
        SparkPropertiesSpecification sparkPropertiesSpecification = jobSubmit.withProperties();
        //Insert ExecutorParams for executor creation
        Map<String,String> executorParams = new HashMap<>(jobTypeObj.getParamsForExecutorCreation());
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
            throw new ExecutorException(this,"JOB Failed. Message : "+e.getMessage(),e);
        }
    }

    @Override
    public boolean killJob(String jobId) throws ExecutorException {
        //TODO check for multiple master url's and do the same
        KillJobRequestSpecification killJobRequestSpecification = client.killJob();
        try {
            return killJobRequestSpecification.withSubmissionId(jobId);
        }catch (FailedSparkRequestException e){
            throw new ExecutorException(this,"Kill Job Request Failed. Message:  "+e.getMessage(),e);
        }
    }

    @Override
    public JobState getJobState(String jobId) throws ExecutorException {
        JobStatusRequestSpecification jobStatusRequestSpecification = client.checkJobStatus();
        try{
            return JobState.valueOf(jobStatusRequestSpecification.withSubmissionId(jobId).name());
        } catch (FailedSparkRequestException e) {
            throw new ExecutorException(this,"Unable to get Job Status. Message : "+e.getMessage(),e);
        }
    }

    @Override
    public boolean isRunning(){
        boolean toReturn=false;

        try {
            return "ALIVE".equals(getSparkClusterDetails().getStatus());
        } catch (ExecutorException e) {
            return false;
        }
    }

    private SparkClusterDetailsResponse getSparkClusterDetails() throws ExecutorException {
        try {
            HttpClient httpClient = (HttpClient) DPAASCommUtil.getHttpClient(15000);
            SparkRestClient confClient = SparkRestClient.builder().sparkVersion(client.getSparkVersion()).httpClient(httpClient).masterPort(((SparkClusterConfig)getConf()).getWebUIPort()).masterHost(client.getMasterHost()).build();
            return new SparkClusterDetailsSpecificationImpl(confClient).getSparkClusterDetails();
        }
        catch(Exception e)
        {
            throw new ExecutorException(this,e);
        }
    }

    public static void main(String[] args) throws ExecutorConfigException {
        Executor executor = new SparkCluster(new JSONObject("{\"id\":2,\"name\":\"Cluster1\",\"disabled\":false,\"type\":\"SPARK_CLUSTER\",\"jobs\":[\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"erroraudit\"],\"host\":\"192.168.230.186\",\"port\":\"6066\",\"webUIPort\":\"8090\",\"sparkVersion\":\"2.2.1\",\"mainClass\":\"com.zoho.dpaas.processor.ZDExecutor\",\"appResource\":\"\",\"clusterMode\":\"spark\",\"httpScheme\":\"http\",\"appName\":\"SparkStandAlone\",\"config\":{\"spark.driver.supervise\":\"true\",\"spark.driver.memory\":\"2g\",\"spark.driver.cores\":2,\"spark.executor.cores\":2,\"spark.executor.memory\":\"2g\",\"spark.executor.instances\":2},\"environmentVariables\":{\"SPARK_ENV_LOADED\":\"1\"}}"));
        System.out.println("c");
    }
}
