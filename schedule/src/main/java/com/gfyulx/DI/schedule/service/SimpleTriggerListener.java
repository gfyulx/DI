package com.gfyulx.DI.schedule.service;

import org.quartz.JobExecutionContext;
import org.quartz.Trigger;
import org.quartz.TriggerListener;


public class SimpleTriggerListener implements TriggerListener {
    private String triggerName;

    public SimpleTriggerListener(String striggerName) {
        this.triggerName = striggerName;
    }

    @Override
    public String getName() {
        return triggerName;
    }

    //被调度时触发，和它相关的org.quartz.jobdetail即将执行
    //该方法优先vetoJobExecution()执行
    @Override
    public void triggerFired(Trigger trigger, JobExecutionContext jobExecutionContext) {

    }

    //被调度时触发，和它相关的org.quartz.jobdetail即将执行
    @Override
    public boolean vetoJobExecution(Trigger trigger, JobExecutionContext jobExecutionContext) {
        return false;
    }

    // 被调度时，触发失败时触发
    @Override
    public void triggerMisfired(Trigger trigger) {

    }

    //执行完毕时触发
    @Override
    public void triggerComplete(Trigger trigger, JobExecutionContext jobExecutionContext, int i) {

    }
}
