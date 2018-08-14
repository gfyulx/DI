package com.gfyulx.DI.schedule.service;


import com.gfyulx.DI.schedule.util.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Trigger;
import org.quartz.TriggerUtils;

import java.util.Date;


public class QuartzTask {
    public static String JOB_GROUP_NAME = "group1";

    public static String TRIGGER_GROUP_NAME = "trigger1";

    private String taskid;

    private Trigger trigger;

    private JobDetail jobDetail;

    private Activity activity;

    private Class<TaskExecutor> taskexecutor;

    private Log logger = LogFactory.getLog(this.getClass());

    public QuartzTask(String taskid, Trigger trigger, JobDetail jobdetail, Activity activity) {
        this.taskid = taskid;
        this.trigger = trigger;
        this.jobDetail = jobdetail;
        this.activity = activity;
    }

    public QuartzTask(String taskid, Class<TaskExecutor> taskexecutor, Activity activity) {
        this.taskid = taskid;

        this.taskexecutor = taskexecutor;

        jobDetail = new JobDetail(this.taskid, JOB_GROUP_NAME, this.taskexecutor);//任务名，任务组，任务执行类

        jobDetail.getJobDataMap().put("ACTIVITY", activity);

        if (SystemUtils.isNumeric(activity.getPeriodTime())) {
            int periodtime = Integer.parseInt(activity.getPeriodTime()) * 60;
            trigger = TriggerUtils.makeSecondlyTrigger(periodtime);
            trigger.setName(this.taskid);
            trigger.setStartTime(new Date());
            trigger.setMisfireInstruction(Trigger.INSTRUCTION_RE_EXECUTE_JOB);
            trigger.setJobDataMap(jobDetail.getJobDataMap());
        } else {
            //crontab格式的时间触发器
            try {
                trigger = new CronTrigger(this.taskid, TRIGGER_GROUP_NAME, activity.getPeriodTime());
                trigger.setJobDataMap(jobDetail.getJobDataMap());
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    public String getTaskid() {
        return taskid;
    }

    public void setTaskid(String taskid) {
        this.taskid = taskid;
    }

    public Trigger getTrigger() {
        return trigger;
    }

    public void setTrigger(Trigger trigger) {
        this.trigger = trigger;
    }

    public JobDetail getJobDetail() {
        return jobDetail;
    }

    public void setJobDetail(JobDetail jobDetail) {
        this.jobDetail = jobDetail;
    }

    public Class<TaskExecutor> getTaskexecutor() {
        return taskexecutor;
    }

    public void setTaskexecutor(Class<TaskExecutor> taskexecutor) {
        this.taskexecutor = taskexecutor;
    }

}
