package com.gfyulx.DI.schedule.service;

import org.quartz.SchedulerException;
import org.quartz.SchedulerListener;
import org.quartz.Trigger;

public class SimpleSchedulerListener implements SchedulerListener {
    //Scheduler 在有新的 JobDetail 部署时调用此方法
    @Override
    public void jobScheduled(Trigger trigger) {

    }
    //Scheduler 在有新的 JobDetail卸载时调用此方法
    @Override
    public void jobUnscheduled(String s, String s1) {

    }
    //当一个 Trigger 来到了再也不会触发的状态时调用这个方法。除非这个 Job 已设置成了持久性，否则它就会从 Scheduler 中移除
    @Override
    public void triggerFinalized(Trigger trigger) {

    }
    //Scheduler 调用这个方法是发生在一个 Trigger 或 Trigger 组被暂停时。假如是 Trigger 组的话，triggerName 参数将为 null。
    @Override
    public void triggersPaused(String s, String s1) {

    }
    //Scheduler 调用这个方法是发生成一个 Trigger 或 Trigger 组从暂停中恢复时。假如是 Trigger 组的话，triggerName 参数将为 null。
    @Override
    public void triggersResumed(String s, String s1) {

    }
    //当一个或一组 JobDetail 暂停时调用这个方法
    @Override
    public void jobsPaused(String s, String s1) {

    }
    //当一个或一组 Job 从暂停上恢复时调用这个方法。假如是一个 Job 组，jobName 参数将为 null
    @Override
    public void jobsResumed(String s, String s1) {

    }
    //Scheduler 的正常运行期间产生一个严重错误时调用这个方法
    @Override
    public void schedulerError(String s, SchedulerException e) {

    }
    //Scheduler 调用这个方法用来通知 SchedulerListener Scheduler 将要被关闭
    @Override
    public void schedulerShutdown() {

    }
}
