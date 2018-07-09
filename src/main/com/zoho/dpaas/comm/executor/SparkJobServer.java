package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.ExecutorConfig;
import com.zoho.dpaas.comm.executor.conf.SJSConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.factory.ExecutorFactory;
import com.zoho.dpaas.comm.executor.interfaces.AbstractExecutor;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
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

import static com.zoho.dpaas.comm.util.DPAASCommUtil.JobState;

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
        try {
        sparkClusterExecutor=getSparkClusterExecutor(getConf());
        client = new SparkJobServerClient(((SJSConfig)getConf()).getSjsURL());
            monitor();
            new ExecutorMonitor(this).start();
        }
        catch (Exception e)
        {
            throw new ExecutorConfigException(e);
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
            throw new ExecutorConfigException("Unable to initialize SparkCluster Conf",e);
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
        String existingContext = contextList.getExistingAvailableContext(((SJSConfig) getConf()).getJobTypes().get(jobType));
        if(existingContext!=null && !existingContext.isEmpty())
        {
            return true;
        }
        else
        {
            String newContext=contextList.getNewContext(((SJSConfig) getConf()).getJobTypes().get(jobType));
            if(newContext!=null && !newContext.isEmpty())
            {
                return true;
            }
        }
        return false;

    }
    private String getContextForTheJob(JobType jobtype) throws ExecutorException {
        try {
            String toReturn;
            toReturn = contextList.getExistingAvailableContext(jobtype);
            if (toReturn == null || toReturn.isEmpty()) {
                toReturn = contextList.getNewContext(jobtype);
                HashMap<String,String> params = new HashMap<>(jobtype.getParamsForExecutorCreation());
                String context_factory =((SJSConfig)getConf()).getConfig().get("context-factory");
                if(context_factory != null){
                    params.put("context-factory",context_factory);
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
        String contextName=getContextForTheJob(conf.getJobTypes().get(jobType));
        jobConf.put("context",contextName);
        try{
            String data="input=\"";
            try {
                for(int i=0;i<jobArgs.length;i++){
                    data+= URLEncoder.encode("\""+jobArgs[i]+"\"","UTF-8")+" ";
                }
                data+="\"";
            } catch (UnsupportedEncodingException e) {
                throw new ExecutorException(this,"Encoding error");
            }
            SparkJobResult result = client.startJob(data,jobConf);
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

    public static void main(String[] args) throws ExecutorConfigException, ExecutorException {
        Executor executor = new SparkJobServer(new JSONObject("{\"id\":3,\"name\":\"SJS1\",\"disabled\":false,\"type\":\"SPARK_SJS\",\"priority\":3,\"sparkClusterId\":2,\"sjsURL\":\"http://192.168.230.186:9090\",\"config\":{\"appName\":\"snap\",\"classPath\":\"spark.jobserver.TestSqlJob\",\"context\":\"context2\"},\"jobTypes\":{\"sampletransformation\":{\"jobType\":\"sampletransformation\",\"minPool\":2,\"maxPool\":3},\"datasettransformation\":{\"jobType\":\"datasettransformation\",\"minPool\":2,\"maxPool\":3},\"sampleextract\":{\"jobType\":\"sampleextract\",\"minPool\":2,\"maxPool\":3},\"dsauditstatefile\":{\"jobType\":\"dsauditstatefile\",\"minPool\":2,\"maxPool\":3},\"rawdsaudittransformation\":{\"jobType\":\"rawdsaudittransformation\",\"minPool\":2,\"maxPool\":3},\"samplepreview\":{\"jobType\":\"samplepreview\",\"minPool\":2,\"maxPool\":3},\"erroraudit\":{\"jobType\":\"erroraudit\",\"minPool\":2,\"maxPool\":3}}}"));
        System.out.println((executor).submit("sampletransformation",new String[]{"%5B%7B%22ruleSetList%22%3A%5B%7B%22dsInfo%22%3A%7B%22entity%22%3A%22rawdatasetaudit%22%2C%22entityId%22%3A1000000036291%2C%22dsID%22%3A-1%2C%22alias%22%3A%22DS%22%2C%22iInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fimport_110581201%2Fimport_110581201.csv%22%7D%2C%22options%22%3A%7B%22header%22%3A%22false%22%2C%22detectencoding%22%3A%7B%22url%22%3A%7B%22method%22%3A%22PUT%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.298e62c39cc4a0ca696bf192c347b59a.9251a7df0fe1adf4c5f58f77c22b3a71%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22detectencoding%22%2C%22type%22%3A%22detectencoding%22%2C%22id%22%3A%221000000036291%22%7D%7D%7D%7D%7D%2C%22rules%22%3A%5B%7B%22name%22%3A%22uniquecol%22%2C%22params%22%3A%7B%7D%2C%22entity%22%3A%22rawdatasetaudit%22%2C%22entityId%22%3A1000000036291%7D%2C%7B%22name%22%3A%22header%22%2C%22params%22%3A%7B%22rn%22%3A0%2C%22force%22%3Atrue%7D%2C%22entity%22%3A%22rawdatasetaudit%22%2C%22entityId%22%3A1000000036291%2C%22postExec%22%3A%7B%22inferrer%22%3A%7B%22callBack%22%3A%7B%22url%22%3A%7B%22method%22%3A%22PUT%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.9d6f05fec8aa82c32c82ee8b880441ac.bba25c00befece499bb3d86fe1c761be%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22inferrer%22%2C%22type%22%3A%22rawdsaudittransformation%22%2C%22id%22%3A%221000000036291%22%7D%7D%2C%22output%22%3A%7B%22oInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Frdsa_1000000036291%2Frdsa_1000000036291.parquet%22%7D%2C%22mPath%22%3A%7B%22storage%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Frdsa_1000000036291%2Frdsa_cm_1000000036291.json%22%7D%7D%7D%7D%7D%2C%22profiler%22%3A%7B%22callBack%22%3A%7B%22url%22%3A%7B%22method%22%3A%22PUT%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.40b805c5a82d7bc97112a43d4909acb8.bc5120e87c59bf77ed88edcfc91ac664%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22profiler%22%2C%22type%22%3A%22rawdsaudittransformation%22%2C%22id%22%3A%221000000036291%22%7D%7D%2C%22doQuality%22%3Afalse%2C%22doProfile%22%3Afalse%2C%22output%22%3A%7B%22writeData%22%3Afalse%2C%22oInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Frdsa_1000000036291%2Frdsa_1000000036291.parquet%22%7D%2C%22mPath%22%3A%7B%22storage%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Frdsa_1000000036291%2Frdsa_cm_1000000036291.json%22%7D%7D%7D%7D%7D%7D%7D%5D%2C%22id%22%3A%22rawdatasetaudit_1000000036291%22%7D%2C%7B%22dsInfo%22%3A%7B%22entity%22%3A%22rawdatasetaudit%22%2C%22entityId%22%3A1000000036291%2C%22dsID%22%3A-1%2C%22alias%22%3A%22DS%22%2C%22rsID%22%3A%22rawdatasetaudit_1000000036291%22%7D%2C%22rules%22%3A%5B%7B%22name%22%3A%22sample%22%2C%22params%22%3A%7B%22sampletype%22%3A%22initial%22%7D%2C%22entity%22%3A%22rawdatasetauditsample%22%2C%22entityId%22%3A1000000036337%2C%22postExec%22%3A%7B%22inferrer%22%3A%7B%22callBack%22%3A%7B%22url%22%3A%7B%22method%22%3A%22PUT%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.115b1bc557322fff4d107c6124bec70d.c092c2b7c6e8331ce59f3b1b8fe83d40%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22inferrer%22%2C%22type%22%3A%22sampleextract%22%2C%22id%22%3A%221000000036337%22%7D%7D%2C%22output%22%3A%7B%22oInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fs_1000000036337%2Fs_1000000036337.parquet%22%7D%2C%22mPath%22%3A%7B%22storage%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fs_1000000036337%2Fs_cm_1000000036337.json%22%7D%7D%7D%7D%7D%2C%22profiler%22%3A%7B%22callBack%22%3A%7B%22url%22%3A%7B%22method%22%3A%22PUT%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.c68abeec30a9232cf57a00b976eee413.dc552e7626d410dbdccc5c26291daa54%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22profiler%22%2C%22type%22%3A%22sampleextract%22%2C%22id%22%3A%221000000036337%22%7D%7D%2C%22fpPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fs_1000000036337%2Fs_p_1000000036337.json%22%7D%2C%22output%22%3A%7B%22writeData%22%3Afalse%2C%22oInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fs_1000000036337%2Fs_1000000036337.parquet%22%7D%2C%22mPath%22%3A%7B%22storage%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fs_1000000036337%2Fs_cm_1000000036337.json%22%7D%7D%7D%7D%7D%7D%7D%5D%2C%22id%22%3A%22rawdatasetauditsample_1000000036337%22%2C%22props%22%3A%7B%22zs.sample%22%3A%22true%22%2C%22zs.ruleset.optimal%22%3A%22true%22%7D%7D%2C%7B%22dsInfo%22%3A%7B%22entity%22%3A%22rawdatasetauditsample%22%2C%22entityId%22%3A1000000036337%2C%22dsID%22%3A1000000036365%2C%22alias%22%3A%22DS%22%2C%22rsID%22%3A%22rawdatasetauditsample_1000000036337%22%7D%2C%22rules%22%3A%5B%7B%22name%22%3A%22copy%22%2C%22params%22%3A%7B%7D%2C%22entity%22%3A%22samplestate%22%2C%22entityId%22%3A1000000036455%2C%22postExec%22%3A%7B%22inferrer%22%3A%7B%22callBack%22%3A%7B%22url%22%3A%7B%22method%22%3A%22POST%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22includeModel%22%3Atrue%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.b427b91c05eb2d82929d7942b0c4c9b3.b077360371fab530633265780685ffd3%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22inferrer%22%2C%22type%22%3A%22sampletransformation%22%2C%22id%22%3A%221000000036455%22%7D%7D%2C%22output%22%3A%7B%22oInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fss_1000000036455%2Fss_1000000036455.parquet%22%7D%2C%22mPath%22%3A%7B%22storage%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fss_1000000036455%2Fss_cm_1000000036455.json%22%7D%7D%7D%7D%7D%2C%22profiler%22%3A%7B%22callBack%22%3A%7B%22url%22%3A%7B%22method%22%3A%22POST%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22includeModel%22%3Atrue%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.b505824a61d9bcdbe2b1b30c71b14497.dd312951f93743ec184996997039758c%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22profiler%22%2C%22type%22%3A%22sampletransformation%22%2C%22id%22%3A%221000000036455%22%7D%7D%2C%22fpPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fss_1000000036455%2Fss_p_1000000036455.json%22%7D%2C%22output%22%3A%7B%22writeData%22%3Afalse%2C%22oInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fss_1000000036455%2Fss_1000000036455.parquet%22%7D%2C%22mPath%22%3A%7B%22storage%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fss_1000000036455%2Fss_cm_1000000036455.json%22%7D%7D%7D%7D%7D%7D%7D%5D%2C%22id%22%3A%22datasetsample_1000000036437%22%2C%22props%22%3A%7B%22zs.sample%22%3A%22true%22%2C%22zs.ruleset.optimal%22%3A%22true%22%7D%7D%5D%7D%5D"}));
        System.out.println("b");
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
