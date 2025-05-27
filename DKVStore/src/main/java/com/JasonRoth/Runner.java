package com.JasonRoth;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Runner {
    static Scanner in = new Scanner(System.in);
    public static void main(String[] args) {
        int port = 8000;
        List<InetSocketAddress> addresses = new ArrayList<>();
        //create addresses from port 8000 to 8090
        for(int i = 0; i < 10; i++) {
            addresses.add(new InetSocketAddress("localhost", port));
            port += 10;
        }
        List<BasicServer> servers = new ArrayList<>();
        //create 10 basic server instances
        for(int i = 0; i < 10; i++) {
            //list of addresses given to the BasicServer instance
            List<InetSocketAddress> addressCopy = new ArrayList<>(addresses);
            //remove the current servers address from the list
            addressCopy.remove(i);
            servers.add(new BasicServer(addresses.get(i).getPort(), addressCopy));
        }
        //Start up the 10 servers
        for(BasicServer server : servers) {
            server.start();
        }

        while(true) {
            String input = in.nextLine();
            if(input.equals("exit")) {
                //stop the 10 servers
                for(BasicServer server : servers) {
                    server.stop();
                }
                break;
            }
        }
        System.out.println("Goodbye!");
    }
}
