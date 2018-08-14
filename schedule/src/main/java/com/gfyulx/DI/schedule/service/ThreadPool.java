package com.gfyulx.DI.schedule.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


public class ThreadPool extends ThreadPoolExecutor {
    private static final Log logger = LogFactory.getLog(ThreadPool.class);

    private final ReentrantLock mainLock = new ReentrantLock();
    private final Condition notFull = mainLock.newCondition();
    // 用于控制线程池满溢时挂起的条件，防止出现在挂起之前，恰好任务全部完成，无法通过afterExecute唤醒
    private boolean isExecute = false;
    private String poolName;


    public ThreadPool(int corePoolSize, int maximumPoolSize, String poolName)
    //初始化线程池，
    {
        super(corePoolSize, maximumPoolSize, 5 * 60, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),//可替换为(r,executor) ->{}
                new RejectedExecutionHandler() {
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        if (!executor.isShutdown()) {
                            // 遇到线程池满溢，则挂起线程池
                            ThreadPool threadPool = (ThreadPool) executor;
                            try {
                                threadPool.awaitExecute();
                                executor.execute(r);
                            } catch (InterruptedException e) {
                                logger.info("挂起任务中断，线程池execute退出");
                            }
                        }
                    }
                });
        this.poolName = poolName;
    }

    public void awaitExecute() throws InterruptedException {
        // 挂起线程，等待其他线程完成后的singnal()信号
        mainLock.lock();
        try {
            while (isExecute) {
                // 挂起一段时间后再重试一次，防止万一锁死
                notFull.await(15, TimeUnit.SECONDS);
            }
        } finally {
            mainLock.unlock();
        }
    }

    @Override
    public void execute(Runnable command) {
        //满溢状态任务得到线程运行时，假定线程池仍为满的
        this.isExecute = true;
        super.execute(command);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        // 执行完毕，说明队列空出一个位置可以恢复执行了
        mainLock.lock();
        try {
            this.isExecute = false;
            notFull.signal();
        } finally {
            mainLock.unlock();
        }
        super.afterExecute(r, t);
    }

    @Override
    protected void terminated() {
        logger.info(this.poolName + "线程池退出");
        super.terminated();
    }
}
