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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles put requests for the key value store
 */
public class PutHandler implements HttpHandler {
    private Map<String, String> dataStore;
    private ConsistentHashingManager hashingManager;
    private final Logger logger;
    private String selfAddressString;

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
            String ownerNode = hashingManager.getNodeForKey(kv.getKey());
            logger.log(Level.INFO, "Key Owner Node Address: " + ownerNode);
            //Using Partition Manager
            if(ownerNode.equals(selfAddressString)){
                //The Key belongs to this node partition
                logger.log(Level.INFO, "Processing Put request on this Node");
                dataStore.put(kv.getKey(), kv.getValue());
                ResponseMessage success = new ResponseMessage("Success", kv.getKey());
                String message = mapper.writeValueAsString(success);
                HttpUtils.sendResponse(exchange, 200, message);
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
}
