package com.JasonRoth.Messaging;

import com.JasonRoth.util.HttpUtils;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles internode messages, implemented as a Runnable to allow for concurrent message handling within a thread pool
 */
public class PeerMessageHandler implements Runnable {

    /**
     * enums for the types of messages
     */
    public enum MessageType {
        PING((byte) 0x01),
        PONG((byte) 0x02),

        FORWARD_PUT_REQUEST((byte) 0x03),
        FORWARD_GET_REQUEST((byte) 0x04),
        FORWARD_DELETE_REQUEST((byte) 0x05),

        OPERATION_SUCCESS_RESPONSE((byte) 0x06), // For PUT/DELETE success
        VALUE_RESPONSE((byte) 0x07),           // For GET success, includes the value
        KEY_NOT_FOUND_RESPONSE((byte) 0x08),   // For GET/DELETE if key isn't on authoritative node
        ERROR_RESPONSE((byte) 0x09),           // Generic error from authoritative node

        UNKNOWN((byte) 0xFF);

        private final byte byteCode;

        MessageType(byte byteCode) {
            this.byteCode = byteCode;
        }

        public byte getByteCode() {
            return byteCode;
        }

        //maps the byte code to its MessageTy[pe
        private static final Map<Byte, MessageType> byteToEnumMap = new HashMap<>();

        static {
            for (MessageType type : values()) {
                byteToEnumMap.put(type.byteCode, type);
            }
        }

        /**
         * Gets the MessageType Enum associated with this byte code
         * @param code byte value representing a message
         * @return the MessageType Enum
         */
        public static MessageType fromByteCode(byte code) {
            return byteToEnumMap.getOrDefault(code, UNKNOWN);
        }
    }

    private Socket socket;
    private Logger logger;
    private Map<String, String> dataStore;

    /**
     * Constructor for peer message handler
     * @param socket the socket for the current peer to peer communication
     * @param logger the logger of this node
     * @param dataStore a reference to this node's in-memory database
     */
    public PeerMessageHandler(Socket socket, Logger logger, Map<String, String> dataStore) {
        this.socket = socket;
        //logger coming from tcp server
        this.logger = logger;
        this.dataStore = dataStore;
    }


    @Override
    public void run(){
        Thread currentHandlerThread = Thread.currentThread();
        logger.log(Level.INFO, "PeerMessageHandler ({0}) started for {1}", new Object[]{currentHandlerThread.getName(), socket.getRemoteSocketAddress()});
        try(Socket clientSocket = this.socket;
            DataInputStream dis = new DataInputStream(clientSocket.getInputStream());
            DataOutputStream dos = new DataOutputStream(clientSocket.getOutputStream())) {

            if (currentHandlerThread.isInterrupted()) {
                logger.log(Level.INFO, "Handler thread interrupted before reading message.");
                return;
            }

            //read a completed framed message from the input stream
            PeerMessageFramer.FramedMessage framedMessage = PeerMessageFramer.readNextMessage(dis);
            MessageType messageType = MessageType.fromByteCode(framedMessage.messageType);
            String payloadJson = framedMessage.getPayloadAsString();

            logger.log(Level.INFO, "Received Message - Type: {0}, Payload: {1}", new Object[]{messageType, payloadJson});
            ObjectMapper mapper = new ObjectMapper();
            //Switch statement to handle different message types
            switch (messageType) {
                case PING:
                    //write a PONG message back
                    PeerMessageFramer.writeMessage(dos, MessageType.PONG.getByteCode(), null);
                    logger.log(Level.INFO, "Pong sent to {0}.", new Object[]{clientSocket.getRemoteSocketAddress()});
                    break;
                case FORWARD_PUT_REQUEST:
                    KeyValue kv = mapper.readValue(payloadJson, KeyValue.class);
                    dataStore.put(kv.getKey(), kv.getValue());
                    ResponseMessage success = new ResponseMessage("Success", kv.getKey());
                    String message = mapper.writeValueAsString(success);
                    PeerMessageFramer.writeMessage(dos, MessageType.OPERATION_SUCCESS_RESPONSE.getByteCode(), message.getBytes(StandardCharsets.UTF_8));
                    break;
                case FORWARD_GET_REQUEST:
                    logger.log(Level.INFO, "FORWARD_GET_REQUEST received. Payload: {0}.", new Object[]{payloadJson});
                    String key = payloadJson;

                    //message should come in as the message type and then the payload is just the key
                    String value = dataStore.get(key);
                    if (value != null) {
                        kv = new KeyValue(key, value);
                        message = mapper.writeValueAsString(kv);
                        PeerMessageFramer.writeMessage(dos, MessageType.VALUE_RESPONSE.getByteCode(), message.getBytes(StandardCharsets.UTF_8));
                    }else{
                        PeerMessageFramer.writeMessage(dos, MessageType.KEY_NOT_FOUND_RESPONSE.getByteCode(), null);
                    }
                    
                    break;
                case FORWARD_DELETE_REQUEST:
                    logger.log(Level.INFO, "FORWARD_DELETE_REQUEST received. Payload {0}.", new Object[]{payloadJson});
                    key = payloadJson;

                    //Message comes in as the message type and the payload is the key we want to delete
                    boolean exists = dataStore.containsKey(key);
                    if(exists){
                        dataStore.remove(key);
                        PeerMessageFramer.writeMessage(dos, MessageType.OPERATION_SUCCESS_RESPONSE.getByteCode(), null);
                    }else{
                        PeerMessageFramer.writeMessage(dos, MessageType.KEY_NOT_FOUND_RESPONSE.getByteCode(), null);
                    }
                    break;
                default:
                    logger.log(Level.WARNING, "Received UNKNOWN or unhandled message type {0}.", new Object[]{messageType});

                    PeerMessageFramer.writeMessage(dos, MessageType.UNKNOWN.getByteCode(), null);
                    break;
            }

        } catch (EOFException e) {
            // This is a normal and expected way for a connection to end cleanly.
            logger.log(Level.INFO, "Peer {0} closed the connection.", socket.getRemoteSocketAddress());
        } catch (IOException e) {
            // Log other, more unexpected I/O errors.
            logger.log(Level.SEVERE, "IOException in PeerMessageHandler for " + socket.getRemoteSocketAddress(), e);
        } catch (Exception e) {
            // Catch any other unexpected runtime exceptions to prevent the thread from dying silently.
            logger.log(Level.SEVERE, "Unexpected Exception in PeerMessageHandler for " + socket.getRemoteSocketAddress(), e);
        } finally {
            // The try-with-resources statement automatically handles closing the socket and streams,
            // so no manual close is needed here.
            logger.log(Level.INFO, "PeerMessageHandler ({0}) finished for {1}", new Object[]{currentHandlerThread.getName(), socket.getRemoteSocketAddress()});
        }
    }
}