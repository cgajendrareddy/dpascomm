package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ywilkof.sparkrestclient.FailedSparkRequestException;
import com.github.ywilkof.sparkrestclient.SparkRestClient;
import com.github.ywilkof.sparkrestclient.interfaces.JobStatusRequestSpecification;
import com.github.ywilkof.sparkrestclient.interfaces.KillJobRequestSpecification;
import com.zoho.dpaas.comm.executor.conf.SparkClusterExecutorConf;
import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;
import com.zoho.dpaas.comm.executor.factory.ExecutorFactory;
import com.zoho.dpaas.comm.executor.interfaces.AbstractDPAASExecutor;
import org.json.JSONObject;

import java.io.IOException;

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
        return null;
    }

    @Override
    public boolean killJob(String jobId) throws DPAASExecutorException {
        //TODO check for multiple master url's and do the same
        SparkClusterExecutor executor= (SparkClusterExecutor) ExecutorFactory.getExecutor(this.getId());
        SparkClusterExecutorConf conf = (SparkClusterExecutorConf) executor.getConf();
        SparkRestClient client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).masterHost(conf.getHost()).masterPort(conf.getPort()).build();
        KillJobRequestSpecification killJobRequestSpecification = client.killJob();
        try {
            return killJobRequestSpecification.withSubmissionId(jobId);
        }catch (FailedSparkRequestException e){
            throw new DPAASExecutorException(executor,"Kill Job Request Failed. Job Status:  "+e.getState(),e);
        }
    }

    @Override
    public JobState getJobState(String jobId) throws DPAASExecutorException {
        SparkClusterExecutor executor= (SparkClusterExecutor) ExecutorFactory.getExecutor(this.getId());
        SparkClusterExecutorConf conf = (SparkClusterExecutorConf) executor.getConf();
        SparkRestClient client = SparkRestClient.builder().sparkVersion(conf.getSparkVersion()).httpScheme(conf.getHttpScheme()).masterHost(conf.getHost()).masterPort(conf.getPort()).build();
        JobStatusRequestSpecification jobStatusRequestSpecification = client.checkJobStatus();
        try{
            return JobState.valueOf(jobStatusRequestSpecification.withSubmissionId(jobId).name());
        } catch (FailedSparkRequestException e) {
            throw new DPAASExecutorException(executor,"Unable to get Job Status. Error Status : "+e.getState(),e);
        }
    }
}
