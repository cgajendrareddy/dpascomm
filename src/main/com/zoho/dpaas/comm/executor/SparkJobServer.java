package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.SJSConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.AbstractExecutor;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import com.zoho.dpaas.comm.executor.list.ContextList;
import org.json.JSONObject;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobResult;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobServerClient;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobServerClientException;

import java.util.HashMap;
import java.util.Map;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

public class SparkJobServer extends AbstractExecutor {

    /**
     * spark cluster for the SJS.
     */
    private final SparkCluster sparkClusterExecutor;
    private SparkJobServerClient client;
    private ContextList contextList;
    private boolean isUp=true;
    /**
     * @param executorConf
     * @throws ExecutorConfigException
     */
    public SparkJobServer(JSONObject executorConf) throws ExecutorConfigException {
        super(getSJSExecutorConf(executorConf));
        sparkClusterExecutor=getSparkClusterExecutor(executorConf);
        client = new SparkJobServerClient(((SJSConfig)getConf()).getSjsURL());
        new SJSMonitor(this).start();
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
        return contextList.getContextFortheNewJob(((SJSConfig) getConf()).getJobTypes().get(jobType))!=null;
    }

    @Override
    public String submit(String jobType, String[] jobArgs) throws ExecutorException {
        //TODO context for which job is to be submitted dynamically based on job type,context pool management
        //TODO SJSClient accepts only one inputJob as String
        //TODO set appName(context name) , other spark configs in config map in SJSConfig before calling submit
        SJSConfig conf = (SJSConfig) getConf();
        Map<String,String> jobConf=new HashMap<String,String>(conf.getConfig());
        String contextName=null;
        if(contextList!=null && conf!=null && conf.getJobTypes()!=null && conf.getJobTypes().containsKey(jobType))
        {
            contextName=contextList.getContextFortheNewJob(conf.getJobTypes().get(jobType));
            jobConf.put("context",contextName);
        }


        try{
            SparkJobResult result = client.startJob(jobArgs.toString(),conf.getConfig());
            return result.getJobId();
        } catch (SparkJobServerClientException e) {
            throw new ExecutorException(this,"Job Submit Failed. Message : "+e.getMessage(),e);
        }

    }

    @Override
    public boolean killJob(String jobId) throws ExecutorException {
        try {
            return client.killJob(jobId);
        } catch (SparkJobServerClientException e) {
            throw new ExecutorException(this,"Unable to kill Job. Message :"+e.getMessage(),e);
        }
    }

    @Override
    public JobState getJobState(String jobId) throws ExecutorException {
        try {
            SparkJobResult response = client.getJobResult(jobId);
            return JobState.valueOf(response.getStatus());
        } catch (SparkJobServerClientException e) {
            throw new ExecutorException(this,"Error in getting JobStatus. Message : "+e.getMessage(),e);
        }
    }

    @Override
    public boolean isRunning() {
        return isUp;
    }

    public static void main(String[] args) throws ExecutorConfigException {
        Executor executor = new SparkJobServer(new JSONObject("{\"id\":4,\"name\":\"SJS1\",\"disabled\":false,\"type\":\"SPARK_SJS\",\"sparkClusterId\":3,\"jobs\":[\"sampletransformation\",\"datasettransformation\",\"sampleextract\",\"dsauditstatefile\",\"rawdsaudittransformation\",\"samplepreview\",\"erroraudit\"],\"sjsURL\":\"http://192.168.230.186:9090\",\"contextTypes\":[{\"name\":\"sample\",\"configs\":{\"spark.cores.max\":2,\"spark.executor.memory\":\"512m\"},\"min\":\"2\",\"max\":\"10\"},{\"name\":\"audit\",\"configs\":{\"spark.cores.max\":2,\"spark.executor.memory\":\"512m\"},\"min\":2,\"max\":10},{\"name\":\"initial_job\",\"configs\":{\"spark.cores.max\":2,\"spark.executor.memory\":\"1G\"},\"min\":\"2\",\"max\":\"10\"}]}"));
        System.out.println("b");
    }

    public void poll() throws ExecutorException {
        try {
            contextList=new ContextList(client.getContexts(), client.getJobs());
            isUp=true;
        }
        catch (SparkJobServerClientException e)
        {
            isUp=false;
            throw new ExecutorException(this,e);
        }
    }
    class SJSMonitor extends Thread
    {
        private SparkJobServer sjs;
        private SJSMonitor(SparkJobServer sjs)
        {
            super("SJS_MONITOR_"+sjs.getId());
        }
        @Override
        public void run()
        {
            try {
                sjs.poll();
                Thread.sleep(1000);
            } catch (ExecutorException e) {
                e.printStackTrace();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }


        }
    }

}
