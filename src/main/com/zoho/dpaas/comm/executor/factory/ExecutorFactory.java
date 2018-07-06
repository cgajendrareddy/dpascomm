package com.zoho.dpaas.comm.executor.factory;

import com.zoho.dpaas.comm.executor.list.ExecutorsList;
import com.zoho.dpaas.comm.executor.SparkCluster;
import com.zoho.dpaas.comm.executor.SparkJobServer;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import com.zoho.dpaas.comm.executor.interfaces.ExecutorConfigProvider;
import org.json.JSONObject;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.ExecutorType;

public class ExecutorFactory {
    public static final String EXECUTOR_CONFIG_PROVIDER_SYSPROP_KEY="dpaas.comm.executor.config.provider";
    public static final String EXECUTOR_TYPE="type";
    public static final String EXECUTOR_CLASS_NAME = "className";
    public static final String EXECUTOR_CONFIG_CLASS_NAME = "configClassName";
    private static ExecutorConfigProvider executorConfigProvider;
    private static ExecutorsList executorsList;

    public  static Executor getExecutor(int executorId) throws ExecutorConfigException {

        JSONObject executorConfig =getExecutorConfig(executorId);
        ExecutorType executorType=ExecutorType.valueOf(executorConfig.getString(EXECUTOR_TYPE));
        switch (executorType)
        {
            case SPARK_CLUSTER:
                return new SparkCluster(executorConfig);
            case SPARK_SJS:
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

    private static void initializeExecutors()
    {
        // TODO: populate the executors
    }

    private static ExecutorConfigProvider getExecutorConfigProvider()throws ExecutorConfigException
    {
        if(executorConfigProvider==null) {
            String executorConfigProviderClass = System.getProperty(EXECUTOR_CONFIG_PROVIDER_SYSPROP_KEY);
            if (executorConfigProviderClass == null) {
                throw new ExecutorConfigException("System property missing: " + EXECUTOR_CONFIG_PROVIDER_SYSPROP_KEY);
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
