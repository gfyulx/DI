package com.gfyulx.DI.schedule.service;


/**
 * @ClassName: MessageTaskFactory
 * @Description: 生成messageTask的工厂类, 实现new对象也可以依赖注入
 * @Author: cqx
 * @Date: 2018-08-07 10:03
 * @Copyright: Fujian Linewell Software Co., Ltd. All rights reserved.
 * 注意：本内容仅限于福建南威软件股份有限公司内部传阅，禁止外泄以及用于其他的商业目的
 */
public class MessageTaskFactory {
    protected MessageTask createInstance(Object... params) {
        //MessageTask messageTask = new MessageTask((TaskInfo) params[0], (String) params[1]);
        MessageTask messageTask = new MessageTask();
        return messageTask;
    }
}
