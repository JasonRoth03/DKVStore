package com.JasonRoth;

import com.JasonRoth.handlers.DeleteHandler;
import com.JasonRoth.handlers.GetHandler;
import com.JasonRoth.handlers.PutHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Basic Http server to handle the Key Value Stores endpoints
 */
public class BasicServer{
    //Basic in-memory key value store
    public Map<String, String> dataStore;

    private int port;

    private HttpServer server;

    public BasicServer(int port) {
        this.port = port;
        dataStore = new ConcurrentHashMap<>();

        InetSocketAddress serverAddress = new InetSocketAddress(port);

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
        server.createContext("/delete", new DeleteHandler());


    }

    public void start(){
        server.start();
    }

    public void stop(){
        server.stop(0);
    }

}
