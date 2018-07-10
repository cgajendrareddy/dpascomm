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
        return "{" +//No I18N
                "host:'" + host + '\'' +//No I18N
                ", port:" + port +//No I18N
                ", webUIPort:" + webUIPort +//No I18N
                '}';//No I18N
    }
}
