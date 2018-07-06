package com.github.ywilkof.sparkrestclient;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Context {
    @Setter @Getter
    private String starttime;
    @Setter @Getter
    private String id;
    @Setter @Getter
    private String name;
    @Setter @Getter
    private int cores;
    @Setter @Getter
    private String user;
    @Setter @Getter
    private int memoryperslave;
    @Setter @Getter
    private String submitdate;
    @Setter @Getter
    private String state;
    @Setter @Getter
    private Long duration;
}
