package com.zoho.dpaas.comm.executor.job;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter@Getter @ToString
public class JobType {
    private String jobType;
    private int minPool;
    private int maxPool;
}
