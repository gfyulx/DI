package com.gfyulx.DI.schedule.service;



public class MessageTaskFactory {
    protected MessageTask createInstance(Object... params) {
        //MessageTask messageTask = new MessageTask((TaskInfo) params[0], (String) params[1]);
        MessageTask messageTask = new MessageTask();
        return messageTask;
    }
}
