package com.gfyulx.DI.schedule.service;

import org.quartz.*;


public class TaskExecutor implements Job {
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        System.out.println("========Test Quartz begin===========");
        JobDetail jobDetail = jobExecutionContext.getJobDetail();
        //任务所配置的数据映射表
        JobDataMap dataMap = jobDetail.getJobDataMap();
        Activity activity  = (Activity) dataMap.get("ACTIVITY");
        //通过dataMap获取对应任务的流程flowId，调用MessageListener.run方法
        //（预留空，待填充,测试打印日志）
        //activity.getMessageListener().run("");
        System.out.println("========Test Quartz success===========");
    }
}
