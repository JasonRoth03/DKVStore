package com.JasonRoth.util;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Handles internode messages, implemented as a Runnable to allow for concurrent message handling within a thread pool
 */
public class PeerMessageHandler implements Runnable {
    //enums for the types of messages
    public enum MessageType {
        PING((byte) 0x01),
        PONG((byte) 0x01),

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

        public static MessageType fromByteCode(byte code) {
            return byteToEnumMap.getOrDefault(code, UNKNOWN);
        }
    }

    private Socket socket;
    private Logger logger;
    //holds a reference to this node's in memory data store for handling forwarded operations from coordinator nodes
    private Map<String, String> dataStore;

    public PeerMessageHandler(Socket socket, Logger logger, Map<String, String> dataStore) {
        this.socket = socket;
        this.logger = logger;
        this.dataStore = dataStore;
    }


    @Override
    public void run(){
        try {
            if(Thread.currentThread().isInterrupted()){
                return;
            }
            InputStream inputStream = socket.getInputStream();
            String message = new BufferedReader(new InputStreamReader(inputStream)).readLine();
            if(message.equals("ping")){
                logger.log(Level.INFO, "Ping received from {0}", socket.getRemoteSocketAddress());
                OutputStream outputStream = socket.getOutputStream();
                String response = "pong\n";
                outputStream.write(response.getBytes());
                outputStream.flush();
                logger.log(Level.INFO, "Pong sent to {0}", socket.getRemoteSocketAddress());
            }
        } catch (IOException e) {
            logger.log(Level.INFO, "Ping receive failed in pingHandler", e);
            throw new RuntimeException(e);
        }finally {
            try {
                socket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}