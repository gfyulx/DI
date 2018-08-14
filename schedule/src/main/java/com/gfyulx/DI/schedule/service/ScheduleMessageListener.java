package com.gfyulx.DI.schedule.service;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.PostConstruct;


public class ScheduleMessageListener implements MessageListener {
    private static final Log logger = LogFactory.getLog(ScheduleMessageListener.class);
    private ThreadPool threadPool;
    //初始线程池大小,默认10(SLA的Spring外部环境变量还未确定，后期改成依赖注入)
    private int threadPoolCorePoolSize = 10;
    //最大线程池大小,默认100(SLA的Spring外部环境变量还未确定，后期改成依赖注入)
    private int threadPoolMaximumPoolSize = 100;

    private MessageTaskFactory messageTaskFactory;

    @Override
    public void run(String id) {
        //MessageTask messageTask = null;
        //TaskInfo taskInfo = new TaskInfo();
       // messageTask = new MessageTask();
        //threadPool.execute(messageTask);
    }

    @PostConstruct
    public void init() throws SLAException {
        logger.info("启动任务调度线程池");
        threadPool = new ThreadPool(threadPoolCorePoolSize,
                threadPoolMaximumPoolSize, "任务调度");
    }
}
