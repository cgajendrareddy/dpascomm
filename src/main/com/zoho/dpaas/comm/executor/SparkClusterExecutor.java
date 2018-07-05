package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;
import com.github.ywilkof.sparkrestclient.JobSubmitRequestSpecificationImpl;
import com.github.ywilkof.sparkrestclient.SparkRestClient;
import com.github.ywilkof.sparkrestclient.interfaces.JobStatusRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.KillJobRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.SparkPropertiesSpecification;
import com.zoho.dpaas.comm.executor.conf.SparkClusterExecutorConf;
import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractDPAASExecutor;
import com.zoho.dpaas.comm.executor.interfaces.DPAASExecutor;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

public class SparkClusterExecutor extends AbstractDPAASExecutor {


    public SparkClusterExecutor(JSONObject executorConf) throws DPAASExecutorException {
        super(getSparkExecutorConf(executorConf));
    }

    static SparkClusterExecutorConf getSparkExecutorConf(JSONObject executorConf) throws  DPAASExecutorException {
        try {
         return new ObjectMapper().readValue(executorConf.toString(),SparkClusterExecutorConf.class);
        } catch (IOException e){
            throw new DPAASExecutorException(null,"Unable to initialize SparkClusterExecutor Conf",e);
        }
    }

    @Override
    public String submit(String... appArgs) throws DPAASExecutorException {
        //TODO Dynamic handling of cores and memory allocation for context
        SparkClusterExecutor executor= this;
        SparkClusterExecutorConf conf = (SparkClusterExecutorConf) executor.getConf();
        SparkRestClient client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).masterHost(conf.getHost()).masterPort(conf.getPort()).environmentVariables(conf.getEnvironmentVariables()).build();
        JobSubmitRequestSpecificationImpl jobSubmit = new JobSubmitRequestSpecificationImpl(client);
        jobSubmit.appName(conf.getAppName());
        jobSubmit.appResource(conf.getAppResource());
        jobSubmit.mainClass(conf.getMainClass());
        jobSubmit.appArgs(Arrays.asList(appArgs));
        SparkPropertiesSpecification sparkPropertiesSpecification = jobSubmit.withProperties();
        Map<String,String> config = conf.getConfig();
        List<String> configParams = SparkClusterExecutorConf.params;
        for(int i=0;i<configParams.size();i++){
            if(config.get(configParams.get(i)) != null){
                sparkPropertiesSpecification.put(configParams.get(i),config.get(configParams.get(i)));
            }
        }
        try {
            return jobSubmit.submit();
        } catch (FailedSparkRequestException e) {
            throw new DPAASExecutorException(executor,"JOB Failed. Message : "+e.getMessage(),e);
        }
    }

    @Override
    public boolean killJob(String jobId) throws DPAASExecutorException {
        //TODO check for multiple master url's and do the same
        SparkClusterExecutor executor= this;
        SparkClusterExecutorConf conf = (SparkClusterExecutorConf) executor.getConf();
        SparkRestClient client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).masterHost(conf.getHost()).masterPort(conf.getPort()).build();
        KillJobRequestSpecification killJobRequestSpecification = client.killJob();
        try {
            return killJobRequestSpecification.withSubmissionId(jobId);
        }catch (FailedSparkRequestException e){
            throw new DPAASExecutorException(executor,"Kill Job Request Failed. Message:  "+e.getMessage(),e);
        }
    }

    @Override
    public JobState getJobState(String jobId) throws DPAASExecutorException {
        SparkClusterExecutor executor= this;
        SparkClusterExecutorConf conf = (SparkClusterExecutorConf) executor.getConf();
        SparkRestClient client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).masterHost(conf.getHost()).masterPort(conf.getPort()).build();
        JobStatusRequestSpecification jobStatusRequestSpecification = client.checkJobStatus();
        try{
            return JobState.valueOf(jobStatusRequestSpecification.withSubmissionId(jobId).name());
        } catch (FailedSparkRequestException e) {
            throw new DPAASExecutorException(executor,"Unable to get Job Status. Message : "+e.getMessage(),e);
        }
    }

    public static void main(String[] args) throws DPAASExecutorException {
        DPAASExecutor executor = new SparkClusterExecutor(new JSONObject("{\"id\":2,\"name\":\"Cluster1\",\"disabled\":false,\"type\":\"SPARKSDCLUSTER\",\"jobs\":[\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"erroraudit\"],\"host\":\"192.168.230.186\",\"port\":\"6066\",\"webUIPort\":\"8090\",\"sparkVersion\":\"2.2.1\",\"mainClass\":\"com.zoho.dpaas.processor.ZDExecutor\",\"appResource\":\"\",\"clusterMode\":\"spark\",\"httpScheme\":\"http\",\"appName\":\"SparkStandAlone\",\"config\":{\"spark.driver.supervise\":\"true\",\"spark.driver.memory\":\"2g\",\"spark.driver.cores\":2,\"spark.executor.cores\":2,\"spark.executor.memory\":\"2g\",\"spark.executor.instances\":2},\"environmentVariables\":{\"SPARK_ENV_LOADED\":\"1\"}}"));
        System.out.println("c");
    }
}
