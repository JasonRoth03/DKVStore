package com.JasonRoth;

import com.JasonRoth.handlers.DeleteHandler;
import com.JasonRoth.handlers.GetHandler;
import com.JasonRoth.handlers.PutHandler;
import com.JasonRoth.Logging.LoggingServer;
import com.sun.net.httpserver.HttpServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Basic Http server to handle the Key Value Stores endpoints
 */
public class BasicServer implements LoggingServer, Watcher {
    private Map<String, String> dataStore; //Hashmap acts as a basic in-memory key value store
    private Logger logger;
    private TCPServer tcpServer;
    private HttpServer server;
    private String selfAddressString;

    private ZooKeeperManager zkManager;
    private ConsistentHashingManager hashingManager;

    public BasicServer(InetSocketAddress serverAddress, int tcpPort) throws IOException {
        this.selfAddressString = serverAddress.getHostString() + ":" + tcpPort;
        dataStore = new ConcurrentHashMap<>();
        logger = initializeLogging(this.getClass().getCanonicalName() + ":" + serverAddress.getPort());

        tcpServer = new TCPServer(tcpPort, dataStore);

        zkManager = new ZooKeeperManager();
        hashingManager = new ConsistentHashingManager(10);//10 virtual nodes per server

        try{
             server = HttpServer.create(serverAddress, 0);
        }catch (IOException ioe){
            System.err.println("Could not create HttpServer: " + ioe.getMessage());
            System.exit(1);
        }

        //Create server contexts
        server.createContext("/put", new PutHandler(selfAddressString, dataStore, hashingManager, logger)); //endpoint for putting a new key value pair into the datastore
        server.createContext("/get", new GetHandler(selfAddressString, dataStore, hashingManager, logger)); //endpoint for getting a value for a key
        server.createContext("/delete", new DeleteHandler(selfAddressString, dataStore, hashingManager, logger)); //endpoint for deleting a key value pair from the datastore
    }

    public void start() throws IOException, InterruptedException {
        //Connect to zookeeper and register this node
        zkManager.connect();
        try{
            //register and set a watch
            zkManager.registerNode(selfAddressString, this);
            logger.log(Level.INFO, "Node " + selfAddressString + " registered with ZooKeeper");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to register with ZooKeeper", e);
            throw new IOException("Could not start server, ZK registration failed");
        }

        //Coordinated wait. This gives all the other servers a small window to register.
        //This is useful when starting up many servers at once
        try{
            logger.log(Level.INFO, "Waiting for other nodes to register...");
            Thread.sleep(2000); //2-second registration window
        }catch (InterruptedException e){
            logger.log(Level.WARNING, "Startup delay was interrupted");
            Thread.currentThread().interrupt();
        }

        //build the initial hash ring
        try{
            List<String> liveNodes = zkManager.getLiveNodes();
            hashingManager.updateNodes(liveNodes);
            logger.log(Level.INFO, "Initial ring built with " + liveNodes.size() + " nodes: " + liveNodes);
        } catch (KeeperException e) {
            logger.log(Level.SEVERE, "Failed to build initial ring", e);
            throw new IOException("Could not start server, failed to build ZK ring", e);
        }

        server.start(); // starts the server that handles basic http endpoints
        tcpServer.start(); // starts the tcp server that handles internode communication on tcpPort
        logger.log(Level.INFO, "Server started on " + selfAddressString);
    }

    public void stop() throws InterruptedException {
        server.stop(0);
        tcpServer.shutdown();
        zkManager.close();
        logger.log(Level.INFO, "Server stopped.");
    }

    //This is the Watcher callback method
    @Override
    public void process(WatchedEvent event) {
        if(event.getType() == Event.EventType.NodeChildrenChanged && event.getPath().equals(ZooKeeperManager.ZK_NODES_PATH)) {
            try{
                logger.log(Level.INFO, "Node membership changed. Rebuilding hashing ring...");
                List<String> liveNodes = zkManager.getLiveNodes();
                hashingManager.updateNodes(liveNodes); //Re-fetch and update the ring
                logger.log(Level.INFO, "New ring nodes: " + liveNodes);
            }catch (Exception e){
                logger.log(Level.SEVERE, "Error updating node list from ZooKeeper", e);
            }
        }
    }
}
