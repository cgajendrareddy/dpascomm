package com.github.ywilkof.sparkrestclient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties(ignoreUnknown = true) @Setter @Getter
public class Context {
    private String starttime;
    private String id;
    private String name;
    private int cores;
    private String user;
    private int memoryperslave;
    private String submitdate;
    private String state;
    private Long duration;
}
