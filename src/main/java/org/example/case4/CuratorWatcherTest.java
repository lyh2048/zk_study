package org.example.case4;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Curator 提供了三种Watcher(Cache)来监听结点的变化
 * Path Cache：监视一个路径下，孩子节点的创建、删除以及节点数据的更新，产生的事件会传递给注册的PathChildrenCacheListener
 * Node Cache：监视一个节点的创建、更新、删除，并将节点的数据缓存在本地
 * Tree Cache：Path Cache 和 Node Cache 的结合，监视路径下的节点创建、更新、删除事件，并缓存路径下所有孩子节点的数据
 */
public class CuratorWatcherTest {
    private static final Logger log = LoggerFactory.getLogger(CuratorWatcherTest.class);
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
        // 2. Register watcher
        PathChildrenCache watcher = new PathChildrenCache(
                client,
                ZK_PATH,
                true // if cache data
        );
        watcher.getListenable().addListener((cli, event) -> {
            ChildData data = event.getData();
            if (data == null) {
                log.info("No data in event[{}]", event);
            } else {
                log.info("Receive event: type=[{}], path=[{}], data=[{}], stat=[{}]", event.getType(), data.getPath(), new String(data.getData()), data.getStat());
            }
        });
        watcher.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        log.info("Register zk watcher successfully!");
        Thread.sleep(Long.MAX_VALUE);
    }
}
