package com.zoho.dpaas.comm.executor.factory;

import com.zoho.dpaas.comm.executor.LocalSparkExecutor;
import com.zoho.dpaas.comm.executor.SJSExecutor;
import com.zoho.dpaas.comm.executor.SparkClusterExecutor;
import com.zoho.dpaas.comm.executor.conf.ExecutorConfig;
import com.zoho.dpaas.comm.executor.exception.ExecutorConfigException;
import com.zoho.dpaas.comm.executor.exception.ExecutorException;
import com.zoho.dpaas.comm.executor.interfaces.Executor;
import com.zoho.dpaas.comm.executor.interfaces.ExecutorConfigProvider;
import org.json.JSONObject;

import static com.zoho.dpaas.comm.util.DPAASCommUtil.ExecutorType;

public class ExecutorFactory {
    public static final String EXECUTOR_CONFIG_PROVIDER_SYSPROP_KEY="dpaas.comm.executor.config.provider";
    public static final String EXECUTOR_TYPE="type";
    private static ExecutorConfigProvider executorConfigProvider;


    public  static Executor getExecutor(int executorId) throws ExecutorConfigException {

        JSONObject executorConfig =getExecutorConfig(executorId);
        ExecutorType executorType=ExecutorType.valueOf(executorConfig.getString(EXECUTOR_TYPE));
        switch (executorType)
        {
            case LOCAL_SPARK:
                return new LocalSparkExecutor(executorConfig);
            case SPARK_CLUSTER:
                return new SparkClusterExecutor(executorConfig);
            case SPARK_SJS:
                return new SJSExecutor(executorConfig);
        }
            return null;
    }

    private static JSONObject getExecutorConfig(int executorId) throws ExecutorConfigException {
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
        return executorConfigProvider.getExecutorConfig(executorId);
    }


}
