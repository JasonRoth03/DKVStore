package com.JasonRoth.util;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TCPServer extends Thread implements LoggingServer{
    private ServerSocket serverSocket;
    private Logger logger;
    private volatile boolean running = true;
    private Thread currentThread;
    private ExecutorService executor = Executors.newCachedThreadPool();

    public TCPServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
        logger = initializeLogging(this.getClass().getCanonicalName() + "_Port:" + port);
    }

    /**
     * Accepts a TCP connection and hands it off to an instance of ping handler in the thread pool
     */
    @Override
    public void run() {
        currentThread = Thread.currentThread();
        while(running && !this.isInterrupted()) {
            try {
                Socket connection = serverSocket.accept();
                logger.log(Level.INFO, "Accepted connection from " + connection.getRemoteSocketAddress());
                PingHandler pingHandler = new PingHandler(connection);
                executor.execute(pingHandler);
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


    /**
     * Simple class for handling ping messages, allows the handling of pings to take place in a thread pool
     */
    public class PingHandler implements Runnable {
        Socket socket;

        public PingHandler(Socket socket){
            this.socket = socket;
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
}
