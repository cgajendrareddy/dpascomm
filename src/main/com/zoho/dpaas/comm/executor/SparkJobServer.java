//$Id$
package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.ExecutorConfig;
import com.zoho.dpaas.comm.executor.conf.SJSConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.factory.ExecutorFactory;
import com.zoho.dpaas.comm.executor.interfaces.AbstractExecutor;
import com.zoho.dpaas.comm.executor.job.JobType;
import com.zoho.dpaas.comm.executor.list.ContextList;
import com.zoho.dpaas.comm.executor.monitor.ExecutorMonitor;
import com.zoho.dpaas.comm.executor.monitor.Monitorable;
import org.json.JSONObject;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobResult;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobServerClient;
import org.khaleesi.carfield.tools.sparkjobserver.api.SparkJobServerClientException;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import static com.zoho.dpaas.comm.executor.constants.ExecutorConstants.*;
import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

/**
 *
 * Created by naga-6803 on 7/7/2018.
 *
 */
public class SparkJobServer extends AbstractExecutor implements Monitorable {

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
        sparkClusterExecutor=getSparkClusterExecutor(getConf());
        client = new SparkJobServerClient(((SJSConfig)getConf()).getSjsURL());
        try {
            monitor();
        } catch (ExecutorException e) {
            e.printStackTrace();
        }
        finally {
            new ExecutorMonitor(this).start();

        }

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
            throw new ExecutorConfigException("Unable to initialize SparkCluster Conf",e);//No I18N
        }
    }

    /**
     * @param executorConf
     * @return the spark cluster executor configured for the SJS
     * @throws ExecutorConfigException
     */
    private SparkCluster getSparkClusterExecutor(ExecutorConfig executorConf) throws ExecutorConfigException {
        int sparkClusterid = ((SJSConfig)executorConf).getSparkClusterId();
        return (SparkCluster) ExecutorFactory.getExecutor(sparkClusterid);
    }

    @Override
    public boolean isResourcesAvailableFortheJob(String jobType) throws ExecutorException {
        String existingContext = contextList.getExistingAvailableContext(getConf().getJobTypes().get(jobType));
        if(existingContext!=null && !existingContext.isEmpty())
        {
            return true;
        }
        else
        {
            String newContext=contextList.getNewContext(getConf().getJobTypes().get(jobType));
            if(newContext!=null && !newContext.isEmpty())
            {
                return true;
            }
        }
        return false;

    }

    /**
     * Get Context for the specified JobType
     * @param jobtype
     * @return
     * @throws ExecutorException
     */
    private String getContextForTheJob(JobType jobtype) throws ExecutorException {
        try {
            String toReturn;
            toReturn = contextList.getExistingAvailableContext(jobtype);
            if (toReturn == null || toReturn.isEmpty()) {
                toReturn = contextList.getNewContext(jobtype);
                HashMap<String,String> params = new HashMap<>(jobtype.getParamsForExecutorCreation(""));
                String contextFactory =((SJSConfig)getConf()).getContextFactory();
                if(contextFactory != null){
                    params.put(CONTEXTFACTORY,contextFactory);
                }
                try {
                    client.createContext(toReturn,params);
                } catch (SparkJobServerClientException e) {
                    throw new ExecutorException(this,e);
                }
            }
            return toReturn;
        }catch (ExecutorException e)
        {
            throw new ExecutorException(this,e);
        }
    }

    @Override
    public String submit(String jobType, String[] jobArgs) throws ExecutorException {
        //TODO context for which job is to be submitted dynamically based on job type,context pool management
        //TODO SJSClient accepts only one inputJob as String
        //TODO set appName(context name) , other spark configs in config map in SJSConfig before calling submit
        SJSConfig conf = (SJSConfig) getConf();
        Map<String,String> jobConf=new HashMap<String,String>(conf.getConfig());
        JobType jobTypeObj = conf.getJobTypes().get(jobType);
        if(jobTypeObj == null){
            throw new ExecutorException(this," Invalid JobType "+jobType+" for executor Id "+this.getId());//No I18N
        }
        if(isResourcesAvailableFortheJob(jobType)){
            throw new ExecutorException(this," Resources Not Available for executing the Job "+jobType);//No I18N
        }
        String contextName=getContextForTheJob(jobTypeObj);
        jobConf.put(CONTEXT,contextName);
        jobConf.put(CLASSPATH,jobTypeObj.getMainClass()!=null?jobTypeObj.getMainClass():conf.getClassPath());
        try{
            String data=SPARK_INPUT+"\"";
            try {
                for(int i=0;i<jobArgs.length;i++){
                    data+= URLEncoder.encode("\""+jobArgs[i]+"\"",INPUT_DATA_ENCODING)+" ";//No I18N
                }
                data+="\"";
            } catch (UnsupportedEncodingException e) {
                throw new ExecutorException(this,"Encoding error");//No I18N
            }
            SparkJobResult result = client.startJob(data,jobConf);
            return result.getJobId();
        } catch (SparkJobServerClientException e) {
            throw new ExecutorException(this,"Job Submit Failed. Message : "+e.getMessage(),e);//No I18N
        }

    }

    @Override
    public boolean killJob(String jobId) throws ExecutorException {
        try {
            return client.killJob(jobId);
        } catch (SparkJobServerClientException e) {
            throw new ExecutorException(this,"Unable to kill Job. Message :"+e.getMessage(),e);//No I18N
        }
    }

    @Override
    public JobState getJobState(String jobId) throws ExecutorException {
        try {
            SparkJobResult response = client.getJobResult(jobId);
            return JobState.valueOf(response.getStatus());
        } catch (SparkJobServerClientException e) {
            throw new ExecutorException(this,"Error in getting JobStatus. Message : "+e.getMessage(),e);//No I18N
        }
    }

    @Override
    public boolean isRunning() {
        return isUp;
    }

    @Override
    public void setIsRunning(boolean running) {
        this.isUp=running;
    }

    @Override
    public void monitor() throws ExecutorException {
        try {
            contextList=new ContextList(client.getContexts(), client.getJobs());
        }
        catch (SparkJobServerClientException e)
        {
            throw new ExecutorException(this,e);
        }
    }

    @Override
    public String getMonitorName() {
        return this.getType()+getConf().getName();
    }
}
