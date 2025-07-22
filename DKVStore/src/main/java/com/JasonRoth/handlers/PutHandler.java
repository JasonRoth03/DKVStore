package com.JasonRoth.handlers;

import com.JasonRoth.ConsistentHashingManager;
import com.JasonRoth.Messaging.PeerMessageFramer;
import com.JasonRoth.Messaging.PeerMessageHandler;
import com.JasonRoth.util.HttpUtils;
import com.JasonRoth.Messaging.KeyValue;
import com.JasonRoth.Messaging.ResponseMessage;
import com.fasterxml.jackson.databind.JsonMappingException;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Handles put requests for the key value store
 */
public class PutHandler implements HttpHandler {
    private Map<String, String> dataStore;
    private ConsistentHashingManager hashingManager;
    private final Logger logger;
    private String selfAddressString;

    private static final int REPLICATION_FACTOR = 3;
    private static final int QUORUM = (REPLICATION_FACTOR / 2) + 1;
    private static final ExecutorService replicationExecutor = Executors.newCachedThreadPool();

    public PutHandler(String selfAddressString, Map<String, String> dataStore, ConsistentHashingManager hashingManager, Logger logger) {
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
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if(requestMethod.equals("POST") && contentType.equals("application/json")){

            /**
             * Get the JSON body as a KeyValue object. The Format is:
             *{
             *  "key": "some_key",
             *  "value": "some_value"
             * }
             *
             */
            ObjectMapper mapper = new ObjectMapper();

            String requestBody = HttpUtils.readRequestBody(exchange);

            if(requestBody.isEmpty()){ //send error since no request body was provided
                ResponseMessage error = new ResponseMessage("Failed - Request body is empty", "NULL");
                String message = mapper.writeValueAsString(error);
                HttpUtils.sendResponse(exchange, 404, message);
            }
            KeyValue kv = null;
            try{
                kv = mapper.readValue(requestBody, KeyValue.class);
            }catch (JsonMappingException jme){
                ResponseMessage error = new ResponseMessage("Failed to parse request body", "NULL");
                String message = mapper.writeValueAsString(error);
                HttpUtils.sendResponse(exchange, 500, message);
            }
            logger.log(Level.INFO, "Received PUT request for key: " + kv.getKey());

            List<String> responsibleNodes = hashingManager.getNodesForKey(kv.getKey(), REPLICATION_FACTOR);
            if (responsibleNodes.size() < QUORUM) {
                HttpUtils.sendResponse(exchange, 503, "{\"error\":\"Not enough nodes available to meet quorum\"}");
                return;
            }

            String ownerNode = responsibleNodes.get(0);

            logger.log(Level.INFO, "Key Primary Node Address: " + ownerNode);
            //Using Partition Manager
            if(ownerNode.equals(selfAddressString)){
                //The Key belongs to this node partition
                logger.log(Level.INFO, "This node is PRIMARY for key: {0}", kv.getKey());
                // --- QUORUM WRITE LOGIC ---
                final CountDownLatch latch = new CountDownLatch(QUORUM -1);
                final AtomicInteger successCount = new AtomicInteger(1); // Count self as one success
                //Write locally
                dataStore.put(kv.getKey(), kv.getValue());

                //Asynchronously replicate to followers
                List<String> replicas = responsibleNodes.stream().filter(n -> !n.equals(selfAddressString)).toList();
                for(String replicaAddress : replicas){
                    CompletableFuture.runAsync(() -> {
                       if(replicateToNode(replicaAddress, PeerMessageHandler.MessageType.REPLICATE_PUT_REQUEST, requestBody)){
                            successCount.incrementAndGet();
                       }
                       latch.countDown();
                    }, replicationExecutor);
                }

                //wait for qurom of acks or timeout
                try{
                    if(latch.await(5, TimeUnit.SECONDS)){
                        if(successCount.get() >= QUORUM){
                            logger.log(Level.INFO, "Quorum of {0} ACKs received for key {1}. Write successful.", new Object[]{QUORUM, kv.getKey()});
                            ResponseMessage success = new ResponseMessage("Success", kv.getKey());
                            String message = mapper.writeValueAsString(success);
                            HttpUtils.sendResponse(exchange, 200, message);
                        }else{
                            logger.log(Level.WARNING, "Write failed for key {0}, Quorum not met. Successes: {1}", new Object[]{kv.getKey(), successCount.get()});
                            //TODO trigger a rollback
                            HttpUtils.sendResponse(exchange, 500, "{\"error\":\"Write failed, quorum not met\"}");
                        }
                    }else{
                        //timeout occurred
                        logger.log(Level.WARNING, "Write timed out for key {0}. Quorum not met.", kv.getKey());
                        //TODO trigger a rollback
                        HttpUtils.sendResponse(exchange, 504, "{\"error\":\"Write timed out, quorum not met\"}");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.log(Level.SEVERE, "Quorum wait interrupted for key " + kv.getKey(), e);
                    HttpUtils.sendResponse(exchange, 500, "{\"error\":\"Server error during write operation\"}");
                }
                // --- END QUORUM WRITE LOGIC ---
            }else{
                String[] ownerAddressString = ownerNode.split(":");
                String ownerHost = ownerAddressString[0];
                int ownerPort = Integer.parseInt(ownerAddressString[1]);
                try(Socket socket = new Socket(ownerHost, ownerPort);
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    DataInputStream dis = new DataInputStream(socket.getInputStream())){

                    logger.log(Level.INFO, "Forwarding PUT request to " + ownerNode);
                    PeerMessageFramer.writeMessage(dos, PeerMessageHandler.MessageType.FORWARD_PUT_REQUEST.getByteCode(), requestBody.getBytes(StandardCharsets.UTF_8));

                    //get the response back from the owner node
                    PeerMessageFramer.FramedMessage response = PeerMessageFramer.readNextMessage(dis);
                    PeerMessageHandler.MessageType type = PeerMessageHandler.MessageType.fromByteCode(response.messageType);
                    logger.log(Level.INFO, "Received " + type + " from peer: " + ownerNode);
                    ResponseMessage responseMessage = new ResponseMessage(response.getPayloadAsString(), kv.getKey());
                    String message = mapper.writeValueAsString(responseMessage);
                    HttpUtils.sendResponse(exchange, 200, message);
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
