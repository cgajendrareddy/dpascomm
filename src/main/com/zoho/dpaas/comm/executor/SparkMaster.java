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
import com.zoho.dpaas.comm.executor.interfaces.AbstractExecutor;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

public class SparkMaster extends AbstractExecutor {


    /**
     * @param sparkMasterConfig
     * @throws ExecutorConfigException
     */
    public SparkMaster(JSONObject sparkMasterConfig) throws ExecutorConfigException {
        super(getSparkExecutorConf(sparkMasterConfig));
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
            throw new ExecutorConfigException("Unable to initialize SparkMaster Conf",e);
        }
    }

    @Override
    public boolean isResourcesAvailableFortheJob(String jobType) throws ExecutorException {
        return false;
    }

    @Override
    public String submit(String... appArgs) throws ExecutorException {
        //TODO Dynamic handling of cores and memory allocation for context
        SparkClusterConfig conf = (SparkClusterConfig) getConf();
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
            throw new ExecutorException(this,"JOB Failed. Message : "+e.getMessage(),e);
        }
    }

    @Override
    public boolean killJob(String jobId) throws ExecutorException {
        //TODO check for multiple master url's and do the same
        SparkClusterConfig conf = (SparkClusterConfig) getConf();
        SparkRestClient client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).masterHost(conf.getHost()).masterPort(conf.getPort()).build();
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
        SparkRestClient client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).masterHost(conf.getHost()).masterPort(conf.getPort()).build();
        JobStatusRequestSpecification jobStatusRequestSpecification = client.checkJobStatus();
        try{
            return JobState.valueOf(jobStatusRequestSpecification.withSubmissionId(jobId).name());
        } catch (FailedSparkRequestException e) {
            throw new ExecutorException(this,"Unable to get Job Status. Message : "+e.getMessage(),e);
        }
    }

}
