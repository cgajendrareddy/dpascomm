package com.zoho.dpaas.comm.executor.monitor;

import com.zoho.dpaas.comm.executor.exception.ExecutorException;

public class ExecutorMonitor extends Thread{

    private Monitorable monitorable;
    public ExecutorMonitor(Monitorable monitorable){
        super(monitorable.getMonitorName());
        this.monitorable = monitorable;
    }
    @Override
    public void run()
    {
        while(true) {
            try {
                monitorable.monitor();
                monitorable.setIsRunning(true);

            } catch (ExecutorException e) {
                monitorable.setIsRunning(false);
                e.printStackTrace();
            } finally {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }


    }

}
