package org.example.case1;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributeClient {
    private final String connectionString = "localhost:2181";
    private final int sessionTimeout = 2000;
    private ZooKeeper zk;


    public static void main(String[] args) throws Exception {
        DistributeClient client = new DistributeClient();
        // 1. 获取zk连接
        client.getConnection();
        // 2. 监听/lyh_servers下面节点的增加和删除
        client.getServerList();
        // 3. 业务逻辑（睡觉）
        client.business();
    }

    private void getConnection() throws IOException {
        zk = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                try {
                    getServerList();
                } catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void getServerList() throws InterruptedException, KeeperException {
        List<String> children = zk.getChildren("/lyh_servers", true);
        List<String> servers = new ArrayList<>();
        for (String child : children) {
            byte[] data = zk.getData("/lyh_servers/" + child, false, null);
            servers.add(new String(data));
        }
        System.out.println(servers);
    }


    private void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }
}
