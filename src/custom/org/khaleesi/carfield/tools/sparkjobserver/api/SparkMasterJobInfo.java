package org.khaleesi.carfield.tools.sparkjobserver.api;

public class SparkMasterJobInfo {
    SparkJobInfo sparkJobInfo =new SparkJobInfo();
    public SparkJobInfo getSparkJobInfo() {
        return sparkJobInfo;
    }

    public String getStatus() {
        return sparkJobInfo.getStatus();
    }

    public void setStatus(String status) {
        sparkJobInfo.setStatus(status);
    }

    public String getMessage() {
        return sparkJobInfo.getMessage();
    }

    public void setMessage(String message) {
        sparkJobInfo.getMessage();
    }

    public String getErrorClass() {
        return sparkJobInfo.getErrorClass();
    }

    public void setErrorClass(String errorClass) {
        sparkJobInfo.setErrorClass(errorClass);
    }

    public String[] getStack() { return sparkJobInfo.getStack(); }

    public void setStack(String[] stack) { sparkJobInfo.setStack(stack); }

    public String getContext() { return sparkJobInfo.getContext(); }

    public void setContext(String context) {
        sparkJobInfo.setContext(context);
    }

    public String getJobId() { return sparkJobInfo.getJobId(); }

    public void setJobId(String jobId) { sparkJobInfo.setJobId(jobId); }

    public String getDuration() {
        return sparkJobInfo.getDuration();
    }

    public void setDuration(String duration) {
        sparkJobInfo.setDuration(duration);
    }

    public String getClassPath() {
        return sparkJobInfo.getClassPath();
    }

    public void setClassPath(String classPath) {
        sparkJobInfo.setClassPath(classPath);
    }

    public String getStartTime() {
        return sparkJobInfo.getStartTime();
    }

    public void setStartTime(String startTime) {
        sparkJobInfo.setStartTime(startTime);
    }

    @Override
    public String toString() {
        return sparkJobInfo.toString();
    }
}
