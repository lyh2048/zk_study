package org.example.case1;

import org.apache.zookeeper.*;

import java.io.IOException;

public class DistributeServer {
    private final String connectionString = "localhost:2181";
    private final int sessionTimeout = 2000;
    private ZooKeeper zk;

    public static void main(String[] args) throws Exception {
        DistributeServer server = new DistributeServer();
        // 1. 获取zk连接
        server.getConnection();
        // 2. 注册服务器到zk
        server.register(args[0]);
        // 3. 启动业务逻辑（睡觉）
        server.business();
    }

    private void getConnection() throws IOException {
        zk = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    private void register(String hostname) throws InterruptedException, KeeperException {
        zk.create("/lyh_servers/" + hostname, hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + " is online");
    }

    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }
}
