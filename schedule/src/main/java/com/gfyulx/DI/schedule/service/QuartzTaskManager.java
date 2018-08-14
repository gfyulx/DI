package com.gfyulx.DI.schedule.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;


public class QuartzTaskManager {
    private Log logger = LogFactory.getLog(QuartzTaskManager.class);
    //调度器
    public Scheduler _scheduler = null;

    private static class QuartzTaskManagerHoler {
        static QuartzTaskManager instance = new QuartzTaskManager();
    }

    public static QuartzTaskManager getInstance() {
        return QuartzTaskManagerHoler.instance;
    }

    private QuartzTaskManager() {
        _scheduler = createScheduler();
        initScheduler();
    }

    public void initScheduler() {
        // 可以在Scheduler运行后执行jobs
        try {
            //创建并主持全局trigger监听器
            TriggerListener triggerListener = new SimpleTriggerListener("SimpleTriggerListener");
            _scheduler.addGlobalTriggerListener(triggerListener);

            // 创建并注册全局job侦听器
            JobListener jobListener = new SimpleJobListener();
            _scheduler.addGlobalJobListener(jobListener);

            // 创建并注册调度程序侦听器
            SchedulerListener schedulerListener = new SimpleSchedulerListener();
            _scheduler.addSchedulerListener(schedulerListener);
        } catch (SchedulerException e) {
            logger.error(e);
        }
    }

    //重新生成调度器
    public void reCreateScheduler() {
        try {
            if (_scheduler.isShutdown()) {
                logger.info("scheduler is shutdown...get a new scheduler.");
                _scheduler = null;
                _scheduler = createScheduler();
                initScheduler();
            }
        } catch (SchedulerException e) {
            logger.error(e);
        }
    }

    //启动quartz的工作
    public void startQuartz() {
        try {
            _scheduler.start();
        } catch (SchedulerException e) {
            logger.error(e);
        }
    }

    //创建调度器
    private Scheduler createScheduler()  // 创建调度器
    {
        StdSchedulerFactory factory = new StdSchedulerFactory();
        Scheduler scheduler = null;
        try {
            factory.initialize("quartz.properties");
            scheduler = factory.getScheduler();
        } catch (SchedulerException ex) {
            logger.error(ex.getMessage(), ex);
        }
        return  scheduler;
    }

    //添加任务
    public void addTask(QuartzTask task) {
        try {
            _scheduler.scheduleJob(task.getJobDetail(), task.getTrigger());
        } catch (SchedulerException e) {
            logger.info(e);
        }
    }

    //删除任务
    public void removeTask(String taskid) {
        try {
            _scheduler.pauseTrigger(taskid, QuartzTask.TRIGGER_GROUP_NAME);//停止触发器
            logger.info("暂停任务：" + taskid);
            _scheduler.unscheduleJob(taskid, QuartzTask.TRIGGER_GROUP_NAME);//移除触发器
            logger.info("停止调度任务：" + taskid);
            _scheduler.deleteJob(taskid, QuartzTask.JOB_GROUP_NAME);//删除任务
            logger.info("删除任务：" + taskid);
        } catch (SchedulerException e) {
            logger.error(e.getMessage(), e);
        }
    }

    //关闭调度
    public void stopQuartz() {
        try {
            _scheduler.shutdown();
        } catch (SchedulerException e) {
            logger.error(e);
        }
    }

}
