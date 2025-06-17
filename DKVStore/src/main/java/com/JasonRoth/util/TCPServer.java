package com.JasonRoth.util;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TCP server being run on specified port, handles internode communication
 */
public class TCPServer extends Thread implements LoggingServer{
    private ServerSocket serverSocket;
    private Logger logger;
    private volatile boolean running = true;
    private Thread currentThread;
    private ExecutorService executor = Executors.newCachedThreadPool();
    private Map<String, String> dataStore;

    public TCPServer(int port, Map<String, String> dataStore) throws IOException {
        serverSocket = new ServerSocket(port);
        logger = initializeLogging(this.getClass().getCanonicalName() + "_Port:" + port);
        this.dataStore = dataStore;
    }

    /**
     * Accepts a TCP connection and hands it off to an instance of PeerMessageHandler in the thread pool
     */
    @Override
    public void run() {
        currentThread = Thread.currentThread();
        while(running && !this.isInterrupted()) {
            try {
                Socket connection = serverSocket.accept();
                logger.log(Level.INFO, "Accepted connection from " + connection.getRemoteSocketAddress());
                PeerMessageHandler peerMessageHandler = new PeerMessageHandler(connection, logger, dataStore);
                executor.execute(peerMessageHandler);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Shutdown the TCP Server
     */
    public void shutdown(){
        try {
            serverSocket.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        running = false;
        if(currentThread != null){
            currentThread.interrupt();
        }
        executor.shutdownNow();
        logger.log(Level.INFO, "TCPServer shutting down");
    }

}
