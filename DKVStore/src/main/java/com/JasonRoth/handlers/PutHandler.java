package com.JasonRoth.handlers;

import com.JasonRoth.Messaging.PeerMessageFramer;
import com.JasonRoth.Messaging.PeerMessageHandler;
import com.JasonRoth.util.HttpUtils;
import com.JasonRoth.Messaging.KeyValue;
import com.JasonRoth.Messaging.ResponseMessage;
import com.JasonRoth.util.PartitionManager;
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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles put requests for the key value store
 */
public class PutHandler implements HttpHandler {
    private Map<String, String> dataStore;
    private PartitionManager partitionManager;
    private final Logger logger;

    public PutHandler(Map<String, String> dataStore, PartitionManager partitionManager, Logger logger) {
        this.dataStore = dataStore;
        this.partitionManager = partitionManager;
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
            InetSocketAddress ownerNode = partitionManager.getNodeForKey(kv.getKey());
            logger.log(Level.INFO, "self address: " + partitionManager.getSelfAddress());
            //Using Partition Manager
            if(ownerNode.equals(partitionManager.getSelfAddress())){
                //The Key belongs to this node partition
                logger.log(Level.INFO, "Processing Put request on this Node");
                dataStore.put(kv.getKey(), kv.getValue());
                ResponseMessage success = new ResponseMessage("Success", kv.getKey());
                String message = mapper.writeValueAsString(success);
                HttpUtils.sendResponse(exchange, 200, message);
            }else{
                try(Socket socket = new Socket(ownerNode.getHostName(), ownerNode.getPort() + 2);
                    DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                    DataInputStream dis = new DataInputStream(socket.getInputStream())){

                    PeerMessageFramer.writeMessage(dos, PeerMessageHandler.MessageType.FORWARD_PUT_REQUEST.getByteCode(), requestBody.getBytes(StandardCharsets.UTF_8));
                    logger.log(Level.INFO, "Forwarding PUT request to " + ownerNode.getHostName() + ":" + ownerNode.getPort());

                    //get the response back from the owner node
                    PeerMessageFramer.FramedMessage response = PeerMessageFramer.readNextMessage(dis);
                    PeerMessageHandler.MessageType type = PeerMessageHandler.MessageType.fromByteCode(response.messageType);
                    logger.log(Level.INFO, "Received " + type + " from peer: " + ownerNode.getHostName() + ":" + ownerNode.getPort());
                    ResponseMessage responseMessage = new ResponseMessage(response.getPayloadAsString(), kv.getKey());
                    String message = mapper.writeValueAsString(responseMessage);
                    HttpUtils.sendResponse(exchange, 200, message);
                }
            }
        }
    }
}
