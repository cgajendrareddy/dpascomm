package com.zoho.dpaas.comm.executor;

import com.bluebreezecf.tools.sparkjobserver.api.SparkJobServerClientDeleteJobImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.SJSConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractExecutor;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import org.json.JSONObject;
import org.khaleesi.carfield.tools.sparkjobserver.api.ISparkJobServerClient;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobResult;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobServerClientException;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobServerClientFactory;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

public class SparkJobServer extends AbstractExecutor {

    /**
     * spark cluster for the SJS.
     */
    public final SparkCluster sparkClusterExecutor;

    /**
     * @param executorConf
     * @throws ExecutorConfigException
     */
    public SparkJobServer(JSONObject executorConf) throws ExecutorConfigException {
        super(getSJSExecutorConf(executorConf));
        sparkClusterExecutor=getSparkClusterExecutor(executorConf);
    }

    /**
     * @param executorConf
     * @return
     * @throws ExecutorConfigException
     */
    static SJSConfig getSJSExecutorConf(JSONObject executorConf) throws ExecutorConfigException {
        try {
            return new ObjectMapper().readValue(executorConf.toString(),SJSConfig.class);

        } catch (Exception e){
            throw new ExecutorConfigException("Unable to initialize SparkCluster Conf",e);
        }
    }

    /**
     * @param executorConf
     * @return the spark cluster executor configured for the SJS
     * @throws ExecutorConfigException
     */
    private SparkCluster getSparkClusterExecutor(JSONObject executorConf) throws ExecutorConfigException {
        return new SparkCluster(executorConf);
    }

    @Override
    public boolean isResourcesAvailableFortheJob(String jobType) throws ExecutorException {
        return false;
    }

    @Override
    public String submit(String... appArgs) throws ExecutorException {
        //TODO context for which job is to be submitted dynamically based on job type,context pool management
        //TODO SJSClient accepts only one inputJob as String
        //TODO set appName(context name) , other spark configs in config map in SJSConfig before calling submit
        SparkJobServer executor =  this;
        SJSConfig conf = (SJSConfig) executor.getConf();
        String url = conf.getSjsURL();
        try{
            ISparkJobServerClient client = SparkJobServerClientFactory.getInstance().createSparkJobServerClient(url);
            SparkJobResult result = client.startJob(appArgs.toString(),conf.getConfig());
            return result.getJobId();
        } catch (SparkJobServerClientException e) {
            throw new ExecutorException(executor,"Job Submit Failed. Message : "+e.getMessage(),e);
        }

    }

    @Override
    public boolean killJob(String jobId) throws ExecutorException {
        SparkJobServer executor =  this;
        SJSConfig conf = (SJSConfig) executor.getConf();
        String url = conf.getSjsURL();
        SparkJobServerClientDeleteJobImpl sjsDelete = new SparkJobServerClientDeleteJobImpl(url);
        try {
            return sjsDelete.killJob(jobId);
        } catch (SparkJobServerClientException e) {
            throw new ExecutorException(executor,"Unable to kill Job. Message :"+e.getMessage(),e);
        }
    }

    @Override
    public JobState getJobState(String jobId) throws ExecutorException {
        SparkJobServer executor =  this;
        SJSConfig conf = (SJSConfig) executor.getConf();
        String url = conf.getSjsURL();
        try {
            ISparkJobServerClient client = SparkJobServerClientFactory.getInstance().createSparkJobServerClient(url);
            SparkJobResult response = client.getJobResult(jobId);
            return JobState.valueOf(response.getStatus());
        } catch (SparkJobServerClientException e) {
            throw new ExecutorException(executor,"Error in getting JobStatus. Message : "+e.getMessage(),e);
        }
    }

    public static void main(String[] args) throws ExecutorConfigException {
        Executor executor = new SparkJobServer(new JSONObject("{\"id\":4,\"name\":\"SJS1\",\"disabled\":false,\"type\":\"SPARK_SJS\",\"sparkClusterId\":3,\"jobs\":[\"sampletransformation\",\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"samplepreview\",\"erroraudit\"],\"sjsURL\":\"http://192.168.230.186:9090\",\"contextTypes\":[{\"name\":\"sample\",\"configs\":{\"spark.cores.max\":2,\"spark.executor.memory\":\"512m\"},\"min\":\"2\",\"max\":\"10\"},{\"name\":\"audit\",\"configs\":{\"spark.cores.max\":2,\"spark.executor.memory\":\"512m\"},\"min\":2,\"max\":10},{\"name\":\"initial_job\",\"configs\":{\"spark.cores.max\":2,\"spark.executor.memory\":\"1G\"},\"min\":\"2\",\"max\":\"10\"}]}"));
        System.out.println("b");
    }
}
