package com.JasonRoth;

import com.JasonRoth.Logging.LoggingServer;
import com.JasonRoth.Messaging.PeerMessageFramer;
import com.JasonRoth.Messaging.PeerMessageHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * simple pinging class that pings all other peers and receives a response
 */
public class Pinger extends Thread implements LoggingServer {
    private volatile boolean running = true;
    private List<InetSocketAddress> addresses;
    private Thread curentThread = null;
    private int MAX_RETRIES = 5;
    private ObjectMapper mapper;

    //store which addresses have responded
    Map<InetSocketAddress, Boolean> pings = new HashMap<>();

    private Logger logger;

    public Pinger(List<InetSocketAddress> addresses, int port) throws IOException {
        this.addresses = addresses;
        this.logger = initializeLogging(this.getClass().getCanonicalName() + "_Port:" + port);
        this.mapper = new ObjectMapper();
        this.mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }


    /**
     * Send pings to peers, log successful ping and set address map to true.
     * Use backoff and retries on unsuccessful connection to allow servers to start
     */
    @Override
    public void run() {
        curentThread = Thread.currentThread();
        while(running & !this.isInterrupted()){
            if(pings.size() == addresses.size()){
                continue;
            }
            for (InetSocketAddress address : addresses) {
                // Only ping if we haven't already received a successful pong from this address
                if (!pings.getOrDefault(address, false)) {
                    boolean success = sendWithRetry(address.getHostName(), address.getPort() + 2);
                    pings.put(address, success);
                    if (success) {
                        logger.log(Level.INFO, "Peer " + address + " is now marked as reachable.");
                    }
                }
            }
            try {
                logger.log(Level.INFO, "Ping results:" + mapper.writeValueAsString(pings));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean sendWithRetry(String host, int port){
        int attempt = 0;
        int readTimeout = 5000;
        long backOff = 500;
        while(attempt < MAX_RETRIES){
            attempt++;
            Socket socket = null;
            try {
                socket = new Socket(host, port);
                DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
                PeerMessageFramer.writeMessage(dos, PeerMessageHandler.MessageType.PING.getByteCode(), null);
                socket.setSoTimeout(readTimeout);
                //get the response
                try{
                    DataInputStream dis = new DataInputStream(socket.getInputStream());
                    PeerMessageFramer.FramedMessage framedMessage = PeerMessageFramer.readNextMessage(dis);
                    PeerMessageHandler.MessageType responseType = PeerMessageHandler.MessageType.fromByteCode(framedMessage.messageType);
                    if(responseType == PeerMessageHandler.MessageType.PONG){
                        logger.log(Level.INFO, "Pong response from host: " + host + " port: " + port);
                        return true;
                    }
                    socket.close();

                }catch (SocketTimeoutException ste){
                    logger.log(Level.SEVERE, "Attempt " + attempt + " connection timed out: " + host + " port: " + port);
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Attempt " + attempt + " to send ping to " + host + ":" + port, e);
            }

            //this is where retries are exhausted
            if(attempt >= MAX_RETRIES){
                break;
            }

            try{
                sleep(backOff);
            }catch (InterruptedException e){
                logger.log(Level.SEVERE, "Sleep interrupted", e);
                break;
            }

        }
        return false;
    }

    public void shutdown(){
        running = false;
        if(curentThread != null){
            curentThread.interrupt();
        }
        logger.log(Level.INFO, "Shutting down Pinger thread");
    }


}
