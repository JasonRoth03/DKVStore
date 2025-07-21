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
import java.util.Map;
import java.util.logging.*;

/**
 * Handles delete requests for the key value store
 */
public class DeleteHandler implements HttpHandler {
    private String selfAddressString;
    private Map<String, String> dataStore;
    private ConsistentHashingManager hashingManager;
    private Logger logger;

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
            String ownerNode = hashingManager.getNodeForKey(key);
            if(ownerNode.equals(selfAddressString)) {
                boolean exists = dataStore.keySet().contains(key);
                if(exists) {
                    dataStore.remove(key);
                    ResponseMessage success = new ResponseMessage("Success", key);
                    String message = mapper.writeValueAsString(success);
                    HttpUtils.sendResponse(exchange, 200, message);
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
                    logger.log(Level.INFO, "Received " + type + " from peer: " + selfAddressString);
                    String message = response.getPayloadAsString();
                    HttpUtils.sendResponse(exchange, 204, message);
                }
            }
        }
    }
}
