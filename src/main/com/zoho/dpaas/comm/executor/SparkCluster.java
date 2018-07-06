package com.zoho.dpaas.comm.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zoho.dpaas.comm.executor.conf.Master;
import com.zoho.dpaas.comm.executor.conf.SparkClusterConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.exception.HAExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SparkCluster extends HAExecutor {


    /**
     * @param sparkClusterConfig
     * @throws ExecutorConfigException
     */
    public SparkCluster(JSONObject sparkClusterConfig) throws ExecutorConfigException, HAExecutorException {
        super(getSparkMasters(sparkClusterConfig),getSparkExecutorConf(sparkClusterConfig));
    }

    /**
     * @param sparkMasters
     * @throws ExecutorConfigException
     */
    private SparkCluster(List<Executor> sparkMasters,SparkClusterConfig clusterConfig) throws HAExecutorException {
        super( sparkMasters,clusterConfig);
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
            throw new ExecutorConfigException("Unable to initialize SparkCluster Conf",e);
        }
    }

    /**
     * @param sparkClusterConfig
     * @return the list of spark masters
     */
    private static List<Executor> getSparkMasters(JSONObject sparkClusterConfig) throws ExecutorConfigException {

        List<Executor> sparkMasters = null;
        SparkClusterConfig sparkClusterConf=getSparkExecutorConf(sparkClusterConfig);
        List<Master> masters = sparkClusterConf.getMasters();
        JSONObject clusterConfig = new JSONObject(sparkClusterConfig.toString());
        if(clusterConfig.has("masters")){
            clusterConfig.remove("masters");
        } else {
            throw new ExecutorConfigException("Masters cannot be null in Executor Id :"+sparkClusterConf.getId());
        }
        for(Master master:masters){
            JSONObject sparkCluster= new JSONObject(clusterConfig.toString());
            JSONObject masterJSON = new JSONObject(master.toString());
            sparkCluster.put("masters",new JSONArray().put(masterJSON));
            SparkMaster sparkMaster = new SparkMaster(sparkCluster);
            if(sparkMasters == null){
                sparkMasters = new ArrayList<>(5);
            }
            sparkMasters.add(sparkMaster);
        }
        return sparkMasters;
    }



    public static void main(String[] args) throws ExecutorException,ExecutorConfigException, HAExecutorException {
        Executor executor = new SparkCluster(new JSONObject("{\"id\":2,\"name\":\"Cluster1\",\"disabled\":true,\"type\":\"SPARK_CLUSTER\",\"priority\":1,\"jobTypes\":{\"datasettransformation\":{\"jobType\":\"datasettransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},\"sampleextract\":{\"jobType\":\"sampleextract\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},\"dsauditstatefile\":{\"jobType\":\"dsauditstatefile\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2g\"},\"rawdsaudittransformation\":{\"jobType\":\"rawdsaudittransformation\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"},\"erroraudit\":{\"jobType\":\"erroraudit\",\"minPool\":2,\"maxPool\":3,\"cores\":2,\"memory\":\"2G\"}},\"masters\":[{\"host\":\"192.168.230.186\",\"port\":\"6066\",\"webUIPort\":\"8090\"},{\"host\":\"192.168.171.27\",\"port\":\"6066\",\"webUIPort\":\"8090\"}],\"sparkVersion\":\"2.2.1\",\"mainClass\":\"com.zoho.dpaas.processor.ZDExecutor\",\"appResource\":\"\",\"clusterMode\":\"spark\",\"httpScheme\":\"http\",\"appName\":\"SparkStandAlone\",\"config\":{\"spark.driver.supervise\":\"true\",\"spark.driver.memory\":\"2g\",\"spark.driver.cores\":2,\"spark.executor.instances\":2},\"environmentVariables\":{\"SPARK_ENV_LOADED\":\"1\"}}"));
        System.out.println(executor.submit("datasettransformation",new String[]{"%5B%7B%22ruleSetList%22%3A%5B%7B%22dsInfo%22%3A%7B%22entity%22%3A%22rawdatasetaudit%22%2C%22entityId%22%3A1000000036291%2C%22dsID%22%3A-1%2C%22alias%22%3A%22DS%22%2C%22iInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fimport_110581201%2Fimport_110581201.csv%22%7D%2C%22options%22%3A%7B%22header%22%3A%22false%22%2C%22detectencoding%22%3A%7B%22url%22%3A%7B%22method%22%3A%22PUT%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.298e62c39cc4a0ca696bf192c347b59a.9251a7df0fe1adf4c5f58f77c22b3a71%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22detectencoding%22%2C%22type%22%3A%22detectencoding%22%2C%22id%22%3A%221000000036291%22%7D%7D%7D%7D%7D%2C%22rules%22%3A%5B%7B%22name%22%3A%22uniquecol%22%2C%22params%22%3A%7B%7D%2C%22entity%22%3A%22rawdatasetaudit%22%2C%22entityId%22%3A1000000036291%7D%2C%7B%22name%22%3A%22header%22%2C%22params%22%3A%7B%22rn%22%3A0%2C%22force%22%3Atrue%7D%2C%22entity%22%3A%22rawdatasetaudit%22%2C%22entityId%22%3A1000000036291%2C%22postExec%22%3A%7B%22inferrer%22%3A%7B%22callBack%22%3A%7B%22url%22%3A%7B%22method%22%3A%22PUT%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.9d6f05fec8aa82c32c82ee8b880441ac.bba25c00befece499bb3d86fe1c761be%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22inferrer%22%2C%22type%22%3A%22rawdsaudittransformation%22%2C%22id%22%3A%221000000036291%22%7D%7D%2C%22output%22%3A%7B%22oInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Frdsa_1000000036291%2Frdsa_1000000036291.parquet%22%7D%2C%22mPath%22%3A%7B%22storage%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Frdsa_1000000036291%2Frdsa_cm_1000000036291.json%22%7D%7D%7D%7D%7D%2C%22profiler%22%3A%7B%22callBack%22%3A%7B%22url%22%3A%7B%22method%22%3A%22PUT%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.40b805c5a82d7bc97112a43d4909acb8.bc5120e87c59bf77ed88edcfc91ac664%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22profiler%22%2C%22type%22%3A%22rawdsaudittransformation%22%2C%22id%22%3A%221000000036291%22%7D%7D%2C%22doQuality%22%3Afalse%2C%22doProfile%22%3Afalse%2C%22output%22%3A%7B%22writeData%22%3Afalse%2C%22oInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Frdsa_1000000036291%2Frdsa_1000000036291.parquet%22%7D%2C%22mPath%22%3A%7B%22storage%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Frdsa_1000000036291%2Frdsa_cm_1000000036291.json%22%7D%7D%7D%7D%7D%7D%7D%5D%2C%22id%22%3A%22rawdatasetaudit_1000000036291%22%7D%2C%7B%22dsInfo%22%3A%7B%22entity%22%3A%22rawdatasetaudit%22%2C%22entityId%22%3A1000000036291%2C%22dsID%22%3A-1%2C%22alias%22%3A%22DS%22%2C%22rsID%22%3A%22rawdatasetaudit_1000000036291%22%7D%2C%22rules%22%3A%5B%7B%22name%22%3A%22sample%22%2C%22params%22%3A%7B%22sampletype%22%3A%22initial%22%7D%2C%22entity%22%3A%22rawdatasetauditsample%22%2C%22entityId%22%3A1000000036337%2C%22postExec%22%3A%7B%22inferrer%22%3A%7B%22callBack%22%3A%7B%22url%22%3A%7B%22method%22%3A%22PUT%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.115b1bc557322fff4d107c6124bec70d.c092c2b7c6e8331ce59f3b1b8fe83d40%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22inferrer%22%2C%22type%22%3A%22sampleextract%22%2C%22id%22%3A%221000000036337%22%7D%7D%2C%22output%22%3A%7B%22oInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fs_1000000036337%2Fs_1000000036337.parquet%22%7D%2C%22mPath%22%3A%7B%22storage%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fs_1000000036337%2Fs_cm_1000000036337.json%22%7D%7D%7D%7D%7D%2C%22profiler%22%3A%7B%22callBack%22%3A%7B%22url%22%3A%7B%22method%22%3A%22PUT%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.c68abeec30a9232cf57a00b976eee413.dc552e7626d410dbdccc5c26291daa54%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22profiler%22%2C%22type%22%3A%22sampleextract%22%2C%22id%22%3A%221000000036337%22%7D%7D%2C%22fpPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fs_1000000036337%2Fs_p_1000000036337.json%22%7D%2C%22output%22%3A%7B%22writeData%22%3Afalse%2C%22oInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fs_1000000036337%2Fs_1000000036337.parquet%22%7D%2C%22mPath%22%3A%7B%22storage%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fs_1000000036337%2Fs_cm_1000000036337.json%22%7D%7D%7D%7D%7D%7D%7D%5D%2C%22id%22%3A%22rawdatasetauditsample_1000000036337%22%2C%22props%22%3A%7B%22zs.sample%22%3A%22true%22%2C%22zs.ruleset.optimal%22%3A%22true%22%7D%7D%2C%7B%22dsInfo%22%3A%7B%22entity%22%3A%22rawdatasetauditsample%22%2C%22entityId%22%3A1000000036337%2C%22dsID%22%3A1000000036365%2C%22alias%22%3A%22DS%22%2C%22rsID%22%3A%22rawdatasetauditsample_1000000036337%22%7D%2C%22rules%22%3A%5B%7B%22name%22%3A%22copy%22%2C%22params%22%3A%7B%7D%2C%22entity%22%3A%22samplestate%22%2C%22entityId%22%3A1000000036455%2C%22postExec%22%3A%7B%22inferrer%22%3A%7B%22callBack%22%3A%7B%22url%22%3A%7B%22method%22%3A%22POST%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22includeModel%22%3Atrue%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.b427b91c05eb2d82929d7942b0c4c9b3.b077360371fab530633265780685ffd3%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22inferrer%22%2C%22type%22%3A%22sampletransformation%22%2C%22id%22%3A%221000000036455%22%7D%7D%2C%22output%22%3A%7B%22oInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fss_1000000036455%2Fss_1000000036455.parquet%22%7D%2C%22mPath%22%3A%7B%22storage%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fss_1000000036455%2Fss_cm_1000000036455.json%22%7D%7D%7D%7D%7D%2C%22profiler%22%3A%7B%22callBack%22%3A%7B%22url%22%3A%7B%22method%22%3A%22POST%22%2C%22url%22%3A%22https%3A%2F%2Fnaga-6803.csez.zohocorpin.com%3A8443%2Fworkspace%2Fyoyoportal%2Fstreamlineapi%2Fcallbacks%2Fupdate_job%22%7D%2C%22includeModel%22%3Atrue%2C%22headers%22%3A%7B%22Authorization%22%3A%22Zoho-oauthtoken%201001.b505824a61d9bcdbe2b1b30c71b14497.dd312951f93743ec184996997039758c%22%7D%2C%22params%22%3A%7B%22cbtype%22%3A%22profiler%22%2C%22type%22%3A%22sampletransformation%22%2C%22id%22%3A%221000000036455%22%7D%7D%2C%22fpPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fss_1000000036455%2Fss_p_1000000036455.json%22%7D%2C%22output%22%3A%7B%22writeData%22%3Afalse%2C%22oInfo%22%3A%7B%22fPath%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fss_1000000036455%2Fss_1000000036455.parquet%22%7D%2C%22mPath%22%3A%7B%22storage%22%3A%7B%22primary%22%3A%22hdfs%3A%2F%2Fstreamline-test1%3A9000%2F5651998%2Fstreamline%2Fss_1000000036455%2Fss_cm_1000000036455.json%22%7D%7D%7D%7D%7D%7D%7D%5D%2C%22id%22%3A%22datasetsample_1000000036437%22%2C%22props%22%3A%7B%22zs.sample%22%3A%22true%22%2C%22zs.ruleset.optimal%22%3A%22true%22%7D%7D%5D%7D%5D"}));
        System.out.println("c");
    }
}
