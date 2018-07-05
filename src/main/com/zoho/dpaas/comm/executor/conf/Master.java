package com.zoho.dpaas.comm.executor.conf;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class Master {
    private String host;
    private int port;
    private int webUIPort;

    @Override
    public String toString() {
        return "{" +
                "host:'" + host + '\'' +
                ", port:" + port +
                ", webUIPort:" + webUIPort +
                '}';
    }
}
