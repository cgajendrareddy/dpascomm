package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;
import com.github.ywilkof.sparkrestclient.JobSubmitRequestSpecificationImpl;
import com.github.ywilkof.sparkrestclient.SparkRestClient;
import com.github.ywilkof.sparkrestclient.interfaces.JobStatusRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.KillJobRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.SparkPropertiesSpecification;
import com.zoho.dpaas.comm.executor.conf.SparkClusterConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractDPAASExecutor;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

public class SparkClusterExecutor extends AbstractDPAASExecutor {


    public SparkClusterExecutor(JSONObject executorConf) throws ExecutorConfigException {
        super(getSparkExecutorConf(executorConf));
    }

    static SparkClusterConfig getSparkExecutorConf(JSONObject executorConf) throws ExecutorConfigException {
        try {
         return new ObjectMapper().readValue(executorConf.toString(),SparkClusterConfig.class);
        } catch (IOException e){
            throw new ExecutorConfigException("Unable to initialize SparkClusterExecutor Conf",e);
        }
    }

    @Override
    public String submit(String... appArgs) throws ExecutorException {
        //TODO Dynamic handling of cores and memory allocation for context
        SparkClusterExecutor executor= this;
        SparkClusterConfig conf = (SparkClusterConfig) executor.getConf();
        SparkRestClient client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).masterHost(conf.getHost()).masterPort(conf.getPort()).environmentVariables(conf.getEnvironmentVariables()).build();
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
            throw new ExecutorException(executor,"JOB Failed. Message : "+e.getMessage(),e);
        }
    }

    @Override
    public boolean killJob(String jobId) throws ExecutorException {
        //TODO check for multiple master url's and do the same
        SparkClusterExecutor executor= this;
        SparkClusterConfig conf = (SparkClusterConfig) executor.getConf();
        SparkRestClient client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).masterHost(conf.getHost()).masterPort(conf.getPort()).build();
        KillJobRequestSpecification killJobRequestSpecification = client.killJob();
        try {
            return killJobRequestSpecification.withSubmissionId(jobId);
        }catch (FailedSparkRequestException e){
            throw new ExecutorException(executor,"Kill Job Request Failed. Message:  "+e.getMessage(),e);
        }
    }

    @Override
    public JobState getJobState(String jobId) throws ExecutorException {
        SparkClusterExecutor executor= this;
        SparkClusterConfig conf = (SparkClusterConfig) executor.getConf();
        SparkRestClient client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).masterHost(conf.getHost()).masterPort(conf.getPort()).build();
        JobStatusRequestSpecification jobStatusRequestSpecification = client.checkJobStatus();
        try{
            return JobState.valueOf(jobStatusRequestSpecification.withSubmissionId(jobId).name());
        } catch (FailedSparkRequestException e) {
            throw new ExecutorException(executor,"Unable to get Job Status. Message : "+e.getMessage(),e);
        }
    }

    public static void main(String[] args) throws ExecutorConfigException {
        Executor executor = new SparkClusterExecutor(new JSONObject("{\"id\":2,\"name\":\"Cluster1\",\"disabled\":false,\"type\":\"SPARK_CLUSTER\",\"jobs\":[\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"erroraudit\"],\"host\":\"192.168.230.186\",\"port\":\"6066\",\"webUIPort\":\"8090\",\"sparkVersion\":\"2.2.1\",\"mainClass\":\"com.zoho.dpaas.processor.ZDExecutor\",\"appResource\":\"\",\"clusterMode\":\"spark\",\"httpScheme\":\"http\",\"appName\":\"SparkStandAlone\",\"config\":{\"spark.driver.supervise\":\"true\",\"spark.driver.memory\":\"2g\",\"spark.driver.cores\":2,\"spark.executor.cores\":2,\"spark.executor.memory\":\"2g\",\"spark.executor.instances\":2},\"environmentVariables\":{\"SPARK_ENV_LOADED\":\"1\"}}"));
        System.out.println("c");
    }
}
