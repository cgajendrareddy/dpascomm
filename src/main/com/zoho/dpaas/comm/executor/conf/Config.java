package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true)
public class Config {
    @JsonProperty(value = "spark.executor.instances")
    private Integer num_executors;

    @JsonProperty(value = "num-cpu-cores")
    private Integer num_cpu_cores;

    @JsonProperty(value = "spark.cores.max")
    private Integer spark_cores_max;

    @JsonProperty(value = "mem-per-node")
    private String mem_per_node;

    @JsonProperty(value = "spark.executor.memory")
    private String spark_executor_memory;
}
