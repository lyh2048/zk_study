package org.example.case2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class DistributedLock {
    private final String connectionString = "localhost:2181";
    private final int sessionTimeout = 2000;
    private final ZooKeeper zooKeeper;
    private String waitPath;
    private String currentNode;
    private CountDownLatch connectionLatch = new CountDownLatch(1);
    private CountDownLatch waitLatch = new CountDownLatch(1);

    public DistributedLock() throws Exception {
        // 获取连接
        zooKeeper = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // connectionLatch 如果连接上zk，可以释放
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }
                // waitLatch 需要释放
                if (watchedEvent.getType() == Event.EventType.NodeDeleted && watchedEvent.getPath().equals(waitPath)) {
                    waitLatch.countDown();
                }
            }
        });
        // 等待zk正常连接
        connectionLatch.await();
        // 判断根节点/locks是否存在
        Stat stat = zooKeeper.exists("/locks", false);
        if (stat == null) {
            // 创建根节点
            zooKeeper.create("/locks", "locks".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    /**
     * 对zk加锁
     */
    public void zkLock() {
        try {
            // 创建对应的临时带序号节点
            currentNode = zooKeeper.create("/locks/seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            // 判断创建的节点是否是最小的，如果是获取到锁，如果不是，监听它序号前一个节点
            List<String> children = zooKeeper.getChildren("/locks", false);
            // 如果children只有一个值，那就直接获取锁，如果有多个节点，需要判断，谁最小
            if (children.size() > 1) {
                Collections.sort(children);
                String thisNode = currentNode.substring("/locks/".length());
                int index = children.indexOf(thisNode);
                if (index == -1) {
                    System.out.println("数据异常");
                } else if (index >= 1) {
                    // 需要监听前一个节点
                    waitPath = "/locks/" + children.get(index - 1);
                    zooKeeper.getData(waitPath, true, null);
                    waitLatch.await();
                }
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 对zk解锁
     */
    public void zkUnlock() {
        // 删除节点
        try {
            zooKeeper.delete(currentNode, -1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
