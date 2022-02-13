package org.example.case4;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

public class CuratorClientTest {
    private static final Logger log = LoggerFactory.getLogger(CuratorClientTest.class);
    /**
     * zookeeper info
     */
    private static final String ZK_ADDRESS = "localhost:2181";
    private static final String ZK_PATH = "/zk_test";

    public static void main(String[] args) throws Exception{
        // 1. Connect to zk
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                ZK_ADDRESS,
                new RetryNTimes(10, 5000)
        );
        client.start();
        log.info("zk client start successfully!");
        // 2. Client API Test
        // 2.1 Create Node
        String data = "hello";
        log.info("create {} {}", ZK_PATH, data);
        client.create()
                .creatingParentsIfNeeded()
                .forPath(ZK_PATH, data.getBytes());
        // 2.2 Get Node and Data
        log.info("ls /");
        List<String> root = client.getChildren().forPath("/");
        log.info(root.toString());
        log.info("get {}", ZK_PATH);
        log.info(Arrays.toString(client.getData().forPath(ZK_PATH)));
        // 2.3 Modify Data
        String data1 = "world";
        log.info("set {} {}", ZK_PATH, data1);
        client.setData().forPath(ZK_PATH, data1.getBytes());
        log.info("get {}", ZK_PATH);
        log.info(Arrays.toString(client.getData().forPath(ZK_PATH)));
        // 2.4 Remove Node
        log.info("delete {}", ZK_PATH);
        client.delete().forPath(ZK_PATH);
        log.info("ls /");
        log.info(client.getChildren().forPath("/").toString());
    }
}
