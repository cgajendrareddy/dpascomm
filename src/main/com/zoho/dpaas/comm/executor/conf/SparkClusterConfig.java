package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true)@ToString
public class SparkClusterConfig extends ExecutorConfig {
    private List<Master> masters;
    private String sparkVersion;
    private String classPath;
    private String appResource;
    private String clusterMode;
    private String httpScheme;
    private String appName;
    private Map<String,String> config;
    private Map<String,String> environmentVariables;

    /**
     * Get Host from Master
     * @return
     */
    public String getHost(){
        if(masters != null && masters.size()>0){
            return masters.get(0).getHost();
        } else {
            throw new IllegalArgumentException("No Masters Found in specified SparkCluster Config Id : "+this.getId());
        }
    }

    /**
     * Get Port From Master
     * @return
     */
    public int getPort(){
        if(masters != null && masters.size()>0){
            return masters.get(0).getPort();
        } else {
            throw new IllegalArgumentException("No Masters Found in specified SparkCluster Config Id : "+this.getId());
        }
    }

    /**
     * Get WebUIPort from Master
     * @return
     */
    public int getWebUIPort(){
        if(masters != null && masters.size()>0){
            return masters.get(0).getWebUIPort();
        } else {
            throw new IllegalArgumentException("No Masters Found in specified SparkCluster Config Id : "+this.getId());
        }
    }
}
