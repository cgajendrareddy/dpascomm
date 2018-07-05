package com.github.ywilkof.sparkrestclient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;

import java.util.List;


@Setter(AccessLevel.PACKAGE) @Getter @JsonIgnoreProperties(ignoreUnknown = true)
public class SparkClusterDetailsResponse extends SparkResponse {
    private String status;
    private int cores;
    private int coresused;
    private int memory;
    private int memoryused;
    private String url;
    private List<Worker> workers;
    private List<Context> activeapps;
}
