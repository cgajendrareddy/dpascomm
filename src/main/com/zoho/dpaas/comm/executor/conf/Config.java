package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter @Getter @JsonIgnoreProperties(ignoreUnknown = true) @ToString
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

    @JsonProperty(value = "spark.executor.cores")
    private Integer spark_executor_cores;

    @JsonProperty(value = "spark.driver.cores")
    private Integer spark_driver_cores;

    @JsonProperty(value = "spark.driver.memory")
    private String spark_driver_memory;

    @JsonProperty(value = "spark.driver.supervise")
    private Boolean spark_driver_supervise;
}
