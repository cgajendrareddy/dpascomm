package com.zoho.dpaas.comm.executor;

import com.bluebreezecf.tools.sparkjobserver.api.SparkJobServerClientDeleteJobImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.SJSExecutorConf;
import com.zoho.dpaas.comm.executor.conf.SparkClusterExecutorConf;
import com.zoho.dpaas.comm.executor.exception.DPAASExecutorException;
import com.zoho.dpaas.comm.executor.factory.ExecutorFactory;
import com.zoho.dpaas.comm.executor.interfaces.AbstractDPAASExecutor;
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
        return null;
    }

    @Override
    public boolean killJob(String jobId) throws DPAASExecutorException {
        SparkClusterExecutor executor = (SparkClusterExecutor) ExecutorFactory.getExecutor(this.getId());
        SparkClusterExecutorConf conf = (SparkClusterExecutorConf) executor.getConf();
        String url = conf.getHttpScheme()+"://"+conf.getHost()+":"+conf.getPort();
        SparkJobServerClientDeleteJobImpl sjsDelete = new SparkJobServerClientDeleteJobImpl(url);
        try {
            return sjsDelete.killJob(jobId);
        } catch (SparkJobServerClientException e) {
            throw new DPAASExecutorException(executor,"Unable to kill Job. Error Message :"+e.getMessage(),e);
        }
    }

    @Override
    public JobState getJobState(String jobId) throws DPAASExecutorException {
        SparkClusterExecutor executor = (SparkClusterExecutor) ExecutorFactory.getExecutor(this.getId());
        SparkClusterExecutorConf conf = (SparkClusterExecutorConf) executor.getConf();
        String url = conf.getHttpScheme()+"://"+conf.getHost()+":"+conf.getPort();
        try {
            ISparkJobServerClient client = SparkJobServerClientFactory.getInstance().createSparkJobServerClient(url);
            SparkJobResult response = client.getJobResult(jobId);
            return JobState.valueOf(response.getStatus());
        } catch (SparkJobServerClientException e) {
            throw new DPAASExecutorException(executor,"Error in getting JobStatus. Error Message : "+e.getMessage(),e);
        }
    }
}
