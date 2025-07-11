package com.JasonRoth.util;

import com.JasonRoth.Logging.LoggingServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PartitionManager implements LoggingServer {
    private final List<InetSocketAddress> nodeList;
    private Logger logger;
    private final InetSocketAddress selfAddress;

    public PartitionManager(InetSocketAddress selfAddress, List<InetSocketAddress> peerAddresses) throws IOException {
        this.logger = initializeLogging(this.getClass().getCanonicalName() + "_Port: " + selfAddress.getPort());
        this.selfAddress = selfAddress;
        List<InetSocketAddress> allNodes = new ArrayList<>(peerAddresses);
        allNodes.add(selfAddress);

        //sort to ensure order is identical on all nodes
        // Corrected sorting logic
        Collections.sort(allNodes, Comparator
                .comparing((InetSocketAddress addr) -> addr.getAddress().getHostAddress())
                .thenComparingInt(InetSocketAddress::getPort));

        this.nodeList = Collections.unmodifiableList(allNodes);
        logger.log(Level.INFO, "Initialized partition manager with nodes: " + this.nodeList);
    }

    public InetSocketAddress getSelfAddress() {
        return this.selfAddress;
    }

    public InetSocketAddress getNodeForKey(String key) {
        if(key == null){
            throw new IllegalArgumentException("Key cannot be null");
        }
        int numberOfNodes = this.nodeList.size();
        int nodeIndex = Math.abs(key.hashCode()) % numberOfNodes;
        return this.nodeList.get(nodeIndex);
    }

    public List<InetSocketAddress> getNodes() {
        return this.nodeList;
    }
}
