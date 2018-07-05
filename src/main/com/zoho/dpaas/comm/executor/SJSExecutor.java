package com.zoho.dpaas.comm.executor;

import com.bluebreezecf.tools.sparkjobserver.api.SparkJobServerClientDeleteJobImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.SJSExecutorConf;
import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractDPAASExecutor;
import com.zoho.dpaas.comm.executor.interfaces.DPAASExecutor;
import org.json.JSONObject;
import org.khaleesi.carfield.tools.sparkjobserver.api.ISparkJobServerClient;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobResult;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobServerClientException;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobServerClientFactory;

import java.io.IOException;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

public class SJSExecutor extends AbstractDPAASExecutor {

    public SJSExecutor(JSONObject executorConf) throws DPAASExecutorException {
        super(getSparkExecutorConf(executorConf));
    }

    static SJSExecutorConf getSparkExecutorConf(JSONObject executorConf) throws  DPAASExecutorException {
        try {
            return new ObjectMapper().readValue(executorConf.toString(),SJSExecutorConf.class);
        } catch (IOException e){
            throw new DPAASExecutorException(null,"Unable to initialize SparkClusterExecutor Conf",e);
        }
    }

    @Override
    public String submit(String... appArgs) throws DPAASExecutorException {
        //TODO context for which job is to be submitted dynamically based on job type,context pool management
        //TODO SJSClient accepts only one inputJob as String
        //TODO set appName(context name) , other spark configs in config map in SJSExecutorConf before calling submit
        SJSExecutor executor =  this;
        SJSExecutorConf conf = (SJSExecutorConf) executor.getConf();
        String url = conf.getSjsURL();
        try{
            ISparkJobServerClient client = SparkJobServerClientFactory.getInstance().createSparkJobServerClient(url);
            SparkJobResult result = client.startJob(appArgs.toString(),conf.getConfig());
            return result.getJobId();
        } catch (SparkJobServerClientException e) {
            throw new DPAASExecutorException(executor,"Job Submit Failed. Message : "+e.getMessage(),e);
        }

    }

    @Override
    public boolean killJob(String jobId) throws DPAASExecutorException {
        SJSExecutor executor =  this;
        SJSExecutorConf conf = (SJSExecutorConf) executor.getConf();
        String url = conf.getSjsURL();
        SparkJobServerClientDeleteJobImpl sjsDelete = new SparkJobServerClientDeleteJobImpl(url);
        try {
            return sjsDelete.killJob(jobId);
        } catch (SparkJobServerClientException e) {
            throw new DPAASExecutorException(executor,"Unable to kill Job. Message :"+e.getMessage(),e);
        }
    }

    @Override
    public JobState getJobState(String jobId) throws DPAASExecutorException {
        SJSExecutor executor =  this;
        SJSExecutorConf conf = (SJSExecutorConf) executor.getConf();
        String url = conf.getSjsURL();
        try {
            ISparkJobServerClient client = SparkJobServerClientFactory.getInstance().createSparkJobServerClient(url);
            SparkJobResult response = client.getJobResult(jobId);
            return JobState.valueOf(response.getStatus());
        } catch (SparkJobServerClientException e) {
            throw new DPAASExecutorException(executor,"Error in getting JobStatus. Message : "+e.getMessage(),e);
        }
    }

    public static void main(String[] args) throws DPAASExecutorException {
        DPAASExecutor executor = new SJSExecutor(new JSONObject("{\"id\":4,\"name\":\"SJS1\",\"disabled\":false,\"type\":\"SPARKSJS\",\"sparkClusterId\":3,\"jobs\":[\"sampletransformation\",\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"samplepreview\",\"erroraudit\"],\"sjsURL\":\"http://192.168.230.186:9090\",\"contextTypes\":[{\"name\":\"sample\",\"configs\":{\"spark.cores.max\":2,\"spark.executor.memory\":\"512m\"},\"min\":\"2\",\"max\":\"10\"},{\"name\":\"audit\",\"configs\":{\"spark.cores.max\":2,\"spark.executor.memory\":\"512m\"},\"min\":2,\"max\":10},{\"name\":\"initial_job\",\"configs\":{\"spark.cores.max\":2,\"spark.executor.memory\":\"1G\"},\"min\":\"2\",\"max\":\"10\"}]}"));
        System.out.println("b");
    }
}
