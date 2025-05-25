package com.JasonRoth.util;

import com.sun.net.httpserver.HttpExchange;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class HttpUtils {
    /**
     * Sends any response back to the client using an existing http exchange object
     * @param exchange - the http exchange
     * @param code - status code for the response
     * @param message - message being sent in the response body as bytes
     * @throws IOException
     */
    public static void sendResponse(HttpExchange exchange, int code, String message) throws IOException {
        byte[] responseBytes = message.getBytes();
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(code, responseBytes.length);
        exchange.getResponseBody().write(responseBytes);
        exchange.close();
    }

    public static String readRequestBody(HttpExchange exchange) throws IOException {
        StringBuilder sb = new StringBuilder();
        try(BufferedReader br = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
        }
        return sb.toString();
    }

    public static Map<String, String> getQueryParams(HttpExchange exchange) throws IOException {
        String query = exchange.getRequestURI().getQuery();
        Map<String, String> queryParams = new HashMap<>();
        if(query != null) {
            String[] pairs = query.split("&");
            for(String pair : pairs) {
                String[] kv = pair.split("=");
                if(kv.length == 2) {
                    queryParams.put(kv[0], kv[1]);
                }
            }
        }
        return queryParams;
    }
}
