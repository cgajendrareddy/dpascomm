package com.zoho.dpaas.comm.executor.factory;

import com.zoho.dpaas.comm.executor.SparkCluster;
import com.zoho.dpaas.comm.executor.SparkJobServer;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import com.zoho.dpaas.comm.executor.interfaces.ExecutorConfigProvider;
import com.zoho.dpaas.comm.executor.list.ExecutorsList;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Map;

import static com.zoho.dpaas.comm.executor.constants.ExecutorConstants.*;

public class ExecutorFactory {
    private static ExecutorConfigProvider executorConfigProvider;
    private static ExecutorsList executorsList;

    static {
        try {
            initializeExecutors();
        } catch (ExecutorConfigException e) {
            e.printStackTrace();
        }
    }
    public  static Executor getExecutor(int executorId) throws ExecutorConfigException {
        JSONObject executorConfig =getExecutorConfig(executorId);
        return getExecutor(executorConfig);
    }

    public  static Executor getExecutor(JSONObject executorConfig) throws ExecutorConfigException {


        String executorType=executorConfig.getString(EXECUTOR_TYPE);
        switch (executorType.toUpperCase())
        {
            case EXECUTOR_SPARK_CLUSTER:
                return new SparkCluster(executorConfig);
            case EXECUTOR_SPARK_SJS:
                return new SparkJobServer(executorConfig);
            default:
                try {
                    String className = executorConfig.getString(EXECUTOR_CLASS_NAME);
                    Class mainClass = Class.forName(className);
                    Executor executor = (Executor)mainClass.getDeclaredConstructor(JSONObject.class).newInstance(executorConfig);
                    return executor;
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
            return null;
    }

    public static Executor getExecutor(String jobType) throws ExecutorException {
        return executorsList.getExecutor(jobType);
    }

    private static void initializeExecutors() throws ExecutorConfigException {
        JSONObject executorsConfig = getExecutorConfigProvider().getExecutorConfigs();
        JSONArray executors = executorsConfig.optJSONArray(EXECUTORS);
        Map<Integer,Executor> executorMap = new HashMap<>();
        for(int i=0;i<executors.length();i++){
            JSONObject executorJSON = executors.optJSONObject(i);
            try {
                if (executorJSON != null && !executorJSON.optBoolean(DISABLED)) {
                    Executor executor = getExecutor(executorJSON);
                    if (executorMap.get(executor.getId()) != null) {
                        throw new ExecutorConfigException(" executor Id " + executor.getId() + " should be duplicated");//No I18N
                    }
                    executorMap.put(executor.getId(), executor);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        executorsList = new ExecutorsList(executorMap);
    }

    private static ExecutorConfigProvider getExecutorConfigProvider()throws ExecutorConfigException
    {
        if(executorConfigProvider==null) {
            String executorConfigProviderClass = System.getProperty(EXECUTOR_CONFIG_PROVIDER_SYSPROP_KEY);
            if (executorConfigProviderClass == null) {
                throw new ExecutorConfigException("System property missing: " + EXECUTOR_CONFIG_PROVIDER_SYSPROP_KEY);//No I18N
            }
            try {
                Class c = Class.forName(executorConfigProviderClass);
                executorConfigProvider=(ExecutorConfigProvider)c.newInstance();

            } catch (Exception e) {
                throw new ExecutorConfigException(e);
            }

        }
        return executorConfigProvider;
    }
    private static JSONObject getExecutoConfigs() throws ExecutorConfigException {

        return getExecutorConfigProvider().getExecutorConfigs();
    }

    private static JSONObject getExecutorConfig(int executorId) throws ExecutorConfigException {

        return getExecutorConfigProvider().getExecutorConfig(executorId);
    }
}
