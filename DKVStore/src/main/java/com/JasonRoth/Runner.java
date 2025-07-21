package com.JasonRoth;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Runner {
    static Scanner in = new Scanner(System.in);

    public static void main(String[] args) throws IOException, InterruptedException {
        //-- Configuration --
        int numberOfServers = 5;
        int startingPort = 8000;
        String hostName = "localhost";
        // ------------------------

        List<BasicServer> servers = new ArrayList<>();

        System.out.println("Initalizing " + numberOfServers + " servers...");

        for(int i = 0; i < numberOfServers; i++) {
            int currentHttpPort = startingPort + (i * 10);
            int currentTcpPort = currentHttpPort + 2;
            InetSocketAddress serverAddress = new InetSocketAddress(hostName, currentHttpPort);

            try{
                BasicServer server = new BasicServer(serverAddress, currentTcpPort);
                servers.add(server);
            }catch(IOException e){
                System.err.println("Failed to initalize server on port " + currentHttpPort);
                e.printStackTrace();
            }
        }

        //Start all servers
        for(BasicServer server : servers) {
            try{
                server.start();
            }catch(IOException e){
                System.err.println("Failed to start a serer.");
                e.printStackTrace();
            }
        }

        System.out.println(numberOfServers + " servers started. Type 'exit' to shutdown.");

        //Keep the main thread alive ot listen for the exit command
        while (true) {
            String input = in.nextLine();
            if (input.equalsIgnoreCase("exit")) {
                System.out.println("Shutting down all servers...");
                for (BasicServer server : servers) {
                    try {
                        server.stop();
                    } catch (InterruptedException e) {
                        System.err.println("Failed to stop a server cleanly.");
                        e.printStackTrace();
                    }
                }
                break;
            }
        }
        System.out.println("Goodbye!");
        // Ensure the program exits cleanly, closing background threads
        System.exit(0);
    }
}
