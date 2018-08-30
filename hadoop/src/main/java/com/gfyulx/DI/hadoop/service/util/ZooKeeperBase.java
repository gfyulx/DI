package com.gfyulx.DI.hadoop.service.util;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

/**
 * @ClassName: ZooKeeperBase
 * @Description: TODO (这里用一句话描述这个类的作用)
 * @author: gfyulx
 * @date: 2018/8/30 9:29
 * @Copyright: 2018 gfyulx
 */
public class ZooKeeperBase {
    private String CONNECT_ADDR = "localhost";
    private int SESSION_OUTTIME = 300000;// ms
    ZooKeeper zk;

    //信号量，阻塞程序执行，用于等待zookeeper连接成功，发送成功信号
    static final CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public void ZooKeeperBase(String connectorAddr, int timeout) {
        this.CONNECT_ADDR = connectorAddr;
        this.SESSION_OUTTIME = timeout;
    }

    public void connect() throws Exception {
        ZooKeeper zk = new ZooKeeper(CONNECT_ADDR, SESSION_OUTTIME,
                new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        // 获取事件的状态
                        KeeperState keeperState = event.getState();
                        EventType eventType = event.getType();
                        // 如果是建立连接
                        if (KeeperState.SyncConnected == keeperState) {
                            if (EventType.None == eventType) {
                                // 如果建立连接成功，则发送信号量，让后续阻塞程序向下执行
                                System.out.println("zk 建立连接");
                                connectedSemaphore.countDown();
                            }
                        }
                    }
                });
        // 进行阻塞
        connectedSemaphore.await();
        this.zk = zk;
    }

    // 创建节点
    public String create(String node, String nodeValues) throws Exception {
        //默认创建持久型的节点createmode=presistent
        return zk.create(node, nodeValues.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    //获取节点值
    public String getNode(String node) throws Exception {
        byte[] data = zk.getData(node, false, null);
        return new String(data);
    }

    //修改、设置节点值
    public boolean setNode(String node, String nodeValues) throws Exception {
        zk.setData(node, nodeValues.getBytes(), -1);
        byte[] data = zk.getData(node, false, null);
        if (nodeValues.equals(new String(data))) {
            return true;
        } else
            return false;
    }

    //删除节点
    public boolean delNode(String node) throws Exception {
        zk.delete(node, -1);
        Stat st = zk.exists(node, false);
        if (!st.equals(null)) {
            return false;
        }
        return true;
    }

    //判断节点是否存在
    public boolean isExists(String node) throws Exception {
        Stat st = zk.exists(node, false);
        if (st.equals(null)) {
            return false;
        }
        return true;
    }

    //判断节点是否为叶子节点
    public boolean isNull(String node) throws Exception {
        Stat st = zk.exists(node, false);
        if (st.equals(null)) {
            return true;
        }
        if (st.getNumChildren() == 0)
            return true;
        return false;
    }

    //断开连接
    public void destory() throws Exception {
        this.zk.close();
    }

}
