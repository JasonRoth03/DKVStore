package com.JasonRoth;

import com.JasonRoth.util.LoggingServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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

    //store which addresses have responded
    Map<InetSocketAddress, Boolean> pings = new HashMap<>();

    private Logger logger;

    public Pinger(List<InetSocketAddress> addresses, int port) throws IOException {
        this.addresses = addresses;
        this.logger = initializeLogging(this.getClass().getCanonicalName() + "_Port:" + port);
    }


    /**
     * Send pings to peers, log successful ping and set address map to true.
     * use backoff and retries on unsuccessful connection to allow servers to start
     */
    @Override
    public void run() {
        curentThread = Thread.currentThread();
        while(running & !this.isInterrupted()){
            if(pings.size() == addresses.size()){
                continue;
            }
            for(InetSocketAddress address : addresses){
                //tcp server listens on server address port + 2
                pings.put(address, sendWithRetry("ping\n", address.getHostName(), address.getPort() + 2));
            }
            logger.log(Level.INFO, "Ping results:" + pings);
        }
    }

    private boolean sendWithRetry(String message, String host, int port){
        int attempt = 0;
        int readTimeout = 5000;
        long backOff = 500;
        while(attempt < MAX_RETRIES){
            attempt++;
            Socket socket = null;
            try {
                socket = new Socket(host, port);
                socket.getOutputStream().write(message.getBytes());
                socket.getOutputStream().flush();
                socket.setSoTimeout(readTimeout);
                //get the response
                try{
                    InputStream in = socket.getInputStream();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                    String response = reader.readLine();
                    if(response.equals("pong")){
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
