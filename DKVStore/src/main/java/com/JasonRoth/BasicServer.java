package com.JasonRoth;

import com.JasonRoth.handlers.DeleteHandler;
import com.JasonRoth.handlers.GetHandler;
import com.JasonRoth.handlers.PutHandler;
import com.JasonRoth.util.LoggingServer;
import com.JasonRoth.util.TCPServer;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Basic Http server to handle the Key Value Stores endpoints
 */
public class BasicServer implements LoggingServer {
    //Basic in-memory key value store
    private Map<String, String> dataStore;
    private List<InetSocketAddress> addresses;
    private Logger logger;
    private TCPServer tcpServer;
    private Pinger pinger;

    private int port;
    private HttpServer server;

    public BasicServer(int port, List<InetSocketAddress> addresses) {
        this.port = port;
        dataStore = new ConcurrentHashMap<>();
        this.addresses = addresses;
        logger = Logger.getLogger(this.getClass().getCanonicalName() + ":" + port);
        InetSocketAddress serverAddress = new InetSocketAddress(port);
        try {
            tcpServer = new TCPServer(port + 2, dataStore);
            pinger = new Pinger(addresses, port + 1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try{
             server = HttpServer.create(serverAddress, 0);
        }catch (BindException be){
            System.err.println("Bind exception when creating server");
            be.printStackTrace();
            System.exit(1);
        }catch (IOException ioe){
            System.err.println("I/O exception when creating server");
            ioe.printStackTrace();
            System.exit(1);
        }

        //Create server contexts
        server.createContext("/put", new PutHandler(dataStore));
        server.createContext("/get", new GetHandler(dataStore));
        server.createContext("/delete", new DeleteHandler(dataStore));
    }

    public void start(){
        server.start();
        tcpServer.start();
        pinger.start();
    }

    public void stop(){
        server.stop(0);
        if(tcpServer.isAlive()){
            tcpServer.shutdown();
        }
        if(pinger.isAlive()){
            pinger.shutdown();
        }
    }

}
