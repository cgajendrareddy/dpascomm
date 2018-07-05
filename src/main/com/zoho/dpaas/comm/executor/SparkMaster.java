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
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
        client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).masterHost(conf.getHost()).masterPort(conf.getPort()).environmentVariables(conf.getEnvironmentVariables()).build();
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

        int requiredCores=getConf().getJobTypes().get(jobType).getCores();
        return requiredCores<avaialbleCores;
    }

    @Override
    public String submit(String... appArgs) throws ExecutorException {
        SparkClusterConfig conf = (SparkClusterConfig) getConf();
        JobSubmitRequestSpecificationImpl jobSubmit = new JobSubmitRequestSpecificationImpl(client);
        jobSubmit.appName(conf.getAppName());
        jobSubmit.appResource(conf.getAppResource());
        jobSubmit.mainClass(conf.getMainClass());
        jobSubmit.appArgs(Arrays.asList(appArgs));
        SparkPropertiesSpecification sparkPropertiesSpecification = jobSubmit.withProperties();
        Map<String,String> config = conf.getConfig();
        List<String> configParams = SparkClusterConfig.params;
        for(int i=0;i<configParams.size();i++){
            if(config.get(configParams.get(i)) != null){
                sparkPropertiesSpecification.put(configParams.get(i),config.get(configParams.get(i)));
            }
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
        SparkClusterConfig conf = (SparkClusterConfig) getConf();
        KillJobRequestSpecification killJobRequestSpecification = client.killJob();
        try {
            return killJobRequestSpecification.withSubmissionId(jobId);
        }catch (FailedSparkRequestException e){
            throw new ExecutorException(this,"Kill Job Request Failed. Message:  "+e.getMessage(),e);
        }
    }

    @Override
    public JobState getJobState(String jobId) throws ExecutorException {
        SparkClusterConfig conf = (SparkClusterConfig) getConf();
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
            return new SparkClusterDetailsSpecificationImpl(client).getSparkClusterDetails();
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
