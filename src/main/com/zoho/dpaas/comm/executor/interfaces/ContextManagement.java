package com.zoho.dpaas.comm.executor.interfaces;

import com.zoho.dpaas.comm.executor.list.ContextList;
import com.zoho.dpaas.comm.executor.list.JobList;

public interface ContextManagement {

    public ContextList getContextList();
    public JobList getJobList();
}
