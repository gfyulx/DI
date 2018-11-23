package com.gfyulx.DI.flink.core.threadFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @ClassName:  DTThreadFactory
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date:   2018/11/2 10:38
 *
 * @Copyright: 2018 gfyulx
 *
 */
public class DTThreadFactory implements ThreadFactory {
    private final static AtomicInteger POOL_NUMBER = new AtomicInteger(1);
    private final static AtomicInteger THREAD_NUMBER = new AtomicInteger(1);
    private final ThreadGroup group;
    private final String namePrefix;

    public DTThreadFactory(String factoryName) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() :
                Thread.currentThread().getThreadGroup();
        namePrefix = factoryName + "-pool-" +
                POOL_NUMBER.getAndIncrement() +
                "-thread-";
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r,
                namePrefix + THREAD_NUMBER.getAndIncrement(),
                0);
        if (t.isDaemon()) {
            t.setDaemon(false);
        }
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }
}
