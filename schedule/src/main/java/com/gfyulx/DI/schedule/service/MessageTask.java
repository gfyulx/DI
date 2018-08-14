package com.gfyulx.DI.schedule.service;



import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MessageTask implements Runnable {
    private static final Log logger = LogFactory.getLog(MessageTask.class);

    public MessageTask() {
    }

    /**
    public MessageTask(TaskInfo taskInfo, String id) {
        taskInfo.setId(id);
        this.taskInfo = taskInfo;
    }
*/

    public void run() {
        System.out.println("");
    }
    /**
    public void run() {
        try {
            taskInfoProcessor.process(taskInfo);
            taskInfo.setState(State.SUCCESS);
        } catch (Exception e) {
            taskInfo.setState(State.FAIL);
        } finally {
            if (scheduleTaskCallBack != null) {
                if (taskInfo.getState() == State.SUCCESS) {
                    scheduleTaskCallBack.success(taskInfo);
                } else {
                    scheduleTaskCallBack.fail(taskInfo);
                }
            }
        }
    }
     */
}
