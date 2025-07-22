package com.JasonRoth.handlers;

import com.JasonRoth.ConsistentHashingManager;
import com.JasonRoth.Messaging.PeerMessageFramer;
import com.JasonRoth.Messaging.PeerMessageHandler;
import com.JasonRoth.util.HttpUtils;
import com.JasonRoth.Messaging.ResponseMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;

/**
 * Handles delete requests for the key value store
 */
public class DeleteHandler implements HttpHandler {
    private String selfAddressString;
    private Map<String, String> dataStore;
    private ConsistentHashingManager hashingManager;
    private Logger logger;

    private static final int REPLICATION_FACTOR = 3;
    private static final int QUORUM = (REPLICATION_FACTOR / 2) + 1;
    private static final ExecutorService replicationExecutor = Executors.newCachedThreadPool();

    public DeleteHandler(String selfAddressString, Map<String, String> dataStore, ConsistentHashingManager hashingManager, Logger logger) {
        this.selfAddressString = selfAddressString;
        this.dataStore = dataStore;
        this.hashingManager = hashingManager;
        this.logger = logger;
    }

    /**
     * Handle the given request and generate an appropriate response.
     * See {@link HttpExchange} for a description of the steps
     * involved in handling an exchange.
     *
     * @param exchange the exchange containing the request from the
     *                 client and used to send the response
     * @throws NullPointerException if exchange is {@code null}
     * @throws IOException          if an I/O error occurs
     */
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        ObjectMapper mapper = new ObjectMapper();
        if(requestMethod.equals("DELETE")) {
            Map<String, String> params = HttpUtils.getQueryParams(exchange);
            String key = params.get("key");
            if(key == null) {
                ResponseMessage valueErr = new ResponseMessage("Failed", "NULL");
                String message = mapper.writeValueAsString(valueErr);
                HttpUtils.sendResponse(exchange, 404, message);
            }
            logger.log(Level.INFO, "Received DELETE request for key: " + key);

            List<String> responsibleNodes = hashingManager.getNodesForKey(key, REPLICATION_FACTOR);
            if (responsibleNodes.size() < QUORUM) {
                HttpUtils.sendResponse(exchange, 503, "{\"error\":\"Not enough nodes available to meet quorum\"}");
                return;
            }

            String ownerNode = responsibleNodes.get(0);
            logger.log(Level.INFO, "Key Owner Node Address: " + ownerNode);
            if(ownerNode.equals(selfAddressString)) {
                logger.log(Level.INFO, "This node is PRIMARY for key: {0}", key);
                // --- QUORUM DELETE LOGIC ---
                final CountDownLatch latch = new CountDownLatch(QUORUM - 1);
                final AtomicInteger successCount = new AtomicInteger(1); //count self as one success
                boolean exists = dataStore.keySet().contains(key);
                if(exists) {
                    //delete locally
                    dataStore.remove(key);

                    //asynchronously replicate to followers
                    List<String> replicas = responsibleNodes.stream().filter(n -> !n.equals(selfAddressString)).toList();
                    for(String replicaAddress : replicas) {
                        CompletableFuture.runAsync(() -> {
                            if(replicateToNode(replicaAddress, PeerMessageHandler.MessageType.REPLICATE_DELETE_REQUEST, key)){
                                successCount.incrementAndGet();
                            }
                            latch.countDown();
                        }, replicationExecutor);
                    }

                    //wait for quorum of acks or timeout
                    try{
                        if(latch.await(5, TimeUnit.SECONDS)){
                            if(successCount.get() >= QUORUM) {
                                logger.log(Level.INFO, "Quorum of {0} ACKs received for key {1}. Delete successful.", new Object[]{QUORUM, key});
                                ResponseMessage success = new ResponseMessage("Success", key);
                                String message = mapper.writeValueAsString(success);
                                HttpUtils.sendResponse(exchange, 200, message);
                            }else{
                                logger.log(Level.WARNING, "Delete failed for key {0}, Quorum not met. Successes: {1}", new Object[]{key, successCount.get()});
                                //TODO trigger a rollback
                                HttpUtils.sendResponse(exchange, 500, "{\"error\":\"Delete failed, quorum not met\"}");
                            }
                        }else{
                            //timeout occurred
                            logger.log(Level.WARNING, "Delete timed out for key {0}. Quorum not met.", key);
                            //TODO trigger a rollback
                            HttpUtils.sendResponse(exchange, 504, "{\"error\":\"Delete timed out, quorum not met\"}");
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.log(Level.SEVERE, "Quorum wait interrupted for key " + key, e);
                        HttpUtils.sendResponse(exchange, 500, "{\"error\":\"Server error during delete operation\"}");
                    }
                // --- END QUORUM DELETE LOGIC ---
                }else{
                    ResponseMessage valueErr = new ResponseMessage("Failed", key);
                    String message = mapper.writeValueAsString(valueErr);
                    HttpUtils.sendResponse(exchange, 404, message);
                }
            }else{
                String[] ownerAddressString = ownerNode.split(":");
                String ownerHost = ownerAddressString[0];
                int ownerPort = Integer.parseInt(ownerAddressString[1]);
                //forward delete request to the owner node
                try(Socket socket = new Socket(ownerHost, ownerPort)) {
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    DataInputStream dis = new DataInputStream(socket.getInputStream());
                    PeerMessageFramer.writeMessage(dos, PeerMessageHandler.MessageType.FORWARD_DELETE_REQUEST.getByteCode(), key.getBytes(StandardCharsets.UTF_8));

                    PeerMessageFramer.FramedMessage response = PeerMessageFramer.readNextMessage(dis);
                    PeerMessageHandler.MessageType type = PeerMessageHandler.MessageType.fromByteCode(response.messageType);
                    logger.log(Level.INFO, "Received " + type + " from peer: " + ownerNode);
                    String message = response.getPayloadAsString();
                    HttpUtils.sendResponse(exchange, 204, message);
                }
            }
        }
    }

    /**
     * Helper method now returns a boolean indicating success.
     */
    private boolean replicateToNode(String nodeAddress, PeerMessageHandler.MessageType messageType, String payload) {
        try {
            String[] parts = nodeAddress.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]) + 2;

            // Use a short timeout for replication attempts
            try (Socket socket = new Socket()) {
                socket.connect(new java.net.InetSocketAddress(host, port), 2000); // 2-second connect timeout
                socket.setSoTimeout(3000); // 3-second read timeout

                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                DataInputStream dis = new DataInputStream(socket.getInputStream());

                PeerMessageFramer.writeMessage(dos, messageType.getByteCode(), payload.getBytes(StandardCharsets.UTF_8));

                // Wait for the REPLICATION_ACK from the follower
                PeerMessageFramer.FramedMessage response = PeerMessageFramer.readNextMessage(dis);
                if (response.messageType == PeerMessageHandler.MessageType.REPLICATION_ACK.getByteCode()) {
                    logger.log(Level.INFO, "Successfully replicated {0} to {1}", new Object[]{messageType, nodeAddress});
                    return true;
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Failed to replicate to node " + nodeAddress, e);
        }
        return false;
    }
}
