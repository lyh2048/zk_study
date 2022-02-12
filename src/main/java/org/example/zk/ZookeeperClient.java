package org.example.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZookeeperClient {
    private final String connectString = "localhost:2181";
    private final int sessionTimeout = 2000;
    private ZooKeeper zk;

    @Before
    public void init() throws IOException {
        zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                List<String> children = null;
                try {
                    children = zk.getChildren("/", true);
                    children.forEach(System.out::println);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Test
    public void create() throws InterruptedException, KeeperException {
        zk.create("/lyh", "test.mp4".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void getChildren() throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren("/", true);
        children.forEach(System.out::println);
    }

    @Test
    public void exist() throws InterruptedException, KeeperException {
        Stat exists = zk.exists("/lyh", false);
        System.out.println(exists == null ? "不存在" : "存在");
    }
}
