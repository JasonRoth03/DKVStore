package com.JasonRoth;

import com.JasonRoth.handlers.DeleteHandler;
import com.JasonRoth.handlers.GetHandler;
import com.JasonRoth.handlers.PutHandler;
import com.JasonRoth.Logging.LoggingServer;
import com.JasonRoth.util.PartitionManager;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Basic Http server to handle the Key Value Stores endpoints
 */
public class BasicServer implements LoggingServer {
    private Map<String, String> dataStore; //Hashmap acts as a basic in-memory key value store
    private List<InetSocketAddress> addresses; // List of peer addresses
    private Logger logger;
    private TCPServer tcpServer;
    private Pinger pinger;

    private int port; //
    private HttpServer server;

    public BasicServer(InetSocketAddress serverAddress, int port, List<InetSocketAddress> addresses) throws IOException {
        this.port = port;
        dataStore = new ConcurrentHashMap<>();
        this.addresses = addresses;
        logger = Logger.getLogger(this.getClass().getCanonicalName() + ":" + port);

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
        PartitionManager partitionManager;
        try{
            partitionManager = new PartitionManager(serverAddress, addresses);
        }catch (IOException ioe){
            logger.log(Level.SEVERE, "IO exception when creating partition manager");
            throw new IOException(ioe);
        }

        //Create server contexts
        server.createContext("/put", new PutHandler(dataStore, partitionManager, logger)); //endpoint for putting a new key value pair into the datastore
        server.createContext("/get", new GetHandler(dataStore, partitionManager, logger)); //endpoint for getting a value for a key
        server.createContext("/delete", new DeleteHandler(dataStore, partitionManager, logger)); //endpoint for deleting a key value pair from the datastore
    }

    public void start(){
        server.start(); // starts the server that handles basic http endpoints
        tcpServer.start(); // starts the tcp server that handles internode communication on this.port + 2
        pinger.start(); // Starts the pinger on this.port + 1
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
