package com.gfyulx.DI.schedule;


public interface ITaskActionService {

    //任务初始化
    public <T> T init(Class<T>c) throws InstantiationException,IllegalAccessException;

    //任务提交
    public <T> T devolop(Class<T> c) throws InstantiationException,IllegalAccessException;

    //任务暂停
    public <T>T  suspend(Class<T> c)throws InstantiationException,IllegalAccessException;

    //任务恢复
    public <T>T  resume(Class<T> c)throws InstantiationException,IllegalAccessException;

    //任务中止
    public <T>T kill(Class<T> c )throws InstantiationException,IllegalAccessException;

    //任务状态查询
    public <T>T queryStatus(Class<T>c )throws InstantiationException,IllegalAccessException;

    //任务结果处理(结果更新)
    public <T>T resultDeal(Class<T>c) throws InstantiationException,IllegalAccessException;

}
