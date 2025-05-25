package com.JasonRoth;

import java.util.Scanner;

public class Runner {
    static Scanner in = new Scanner(System.in);
    public static void main(String[] args) {
        BasicServer server = new BasicServer(8080);
        server.start();
        while(true) {
            String input = in.nextLine();
            if(input.equals("exit")) {
                System.out.println("Goodbye!");
                server.stop();
                break;
            }
        }
    }
}
