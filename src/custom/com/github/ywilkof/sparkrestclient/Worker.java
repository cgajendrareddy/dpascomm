package com.github.ywilkof.sparkrestclient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties(ignoreUnknown = true) @Setter @Getter
public class Worker {
    private String id;
    private String host;
    private int port;
    private String webuiaddress;
    private int cores;
    private int coresused;
    private int coresfree;
    private int memory;
    private int memoryused;
    private int memoryfree;
    private String state;
    private Long lastheartbeat;
}
