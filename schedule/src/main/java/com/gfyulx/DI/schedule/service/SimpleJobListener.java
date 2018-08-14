package com.gfyulx.DI.schedule.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;


public class SimpleJobListener implements JobListener {
    Log logger = LogFactory.getLog(SimpleJobListener.class);

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext jobExecutionContext) {
        String jobName = jobExecutionContext.getJobDetail().getName();
        logger.info(jobName + " is about to be executed...");
    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext jobExecutionContext) {
        String jobName = jobExecutionContext.getJobDetail().getName();
        logger.info(jobName + " was vetoed and not executed...");
    }

    @Override
    public void jobWasExecuted(JobExecutionContext jobExecutionContext, JobExecutionException e) {
         String jobName = jobExecutionContext.getJobDetail().getName();
         logger.info(jobName + " was executed...");
    }
}
