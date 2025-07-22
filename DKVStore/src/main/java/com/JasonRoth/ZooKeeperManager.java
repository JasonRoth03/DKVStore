package com.JasonRoth;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperManager {
    private static final String ZK_CONNECTION_STRING = "localhost:2181";
    private static final int SESSION_TIMEOUT = 5000;
    static final String ZK_NODES_PATH = "/dkv_nodes";

    private ZooKeeper zooKeeper;

    public void connect() throws IOException, InterruptedException {
        final CountDownLatch connectedSignal = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(ZK_CONNECTION_STRING, SESSION_TIMEOUT, event -> {
            if(event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });
        connectedSignal.await();
    }

    public void registerNode(String address, Watcher watcher) throws KeeperException, InterruptedException {
        //ensure parent path exists
        if(zooKeeper.exists(ZK_NODES_PATH, false) == null){
            zooKeeper.create(ZK_NODES_PATH, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        //Create an ephemeral znode for this server
        String znodePath = ZK_NODES_PATH + "/" + address;
        if(zooKeeper.exists(znodePath, false) != null){
            //clean up previous stale node if exists
            zooKeeper.delete(znodePath, -1);
        }
        zooKeeper.create(znodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        //Set a watch on the parent nodes path to get notified of changes
        zooKeeper.getChildren(ZK_NODES_PATH, watcher);
    }

    public List<String> getLiveNodes(Watcher watcher) throws KeeperException, InterruptedException {
        return zooKeeper.getChildren(ZK_NODES_PATH, watcher);
    }

    public void close() throws InterruptedException {
        if(zooKeeper != null) {
            zooKeeper.close();
        }
    }
}

