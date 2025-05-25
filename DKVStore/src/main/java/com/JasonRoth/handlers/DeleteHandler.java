package com.JasonRoth.handlers;

import com.JasonRoth.util.HttpUtils;
import com.JasonRoth.util.KeyValue;
import com.JasonRoth.util.ResponseMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.Map;

public class DeleteHandler implements HttpHandler {
    private Map<String, String> dataStore;

    public DeleteHandler(Map<String, String> dataStore) {
        this.dataStore = dataStore;
    }

    /**
     * Handle the given request and generate an appropriate response.
     * See {@link HttpExchange} for a description of the steps
     * involved in handling an exchange.
     *
     * @param exchange the exchange containing the request from the
     *                 client and used to send the response
     * @throws NullPointerException if exchange is {@code null}
     * @throws IOException          if an I/O error occurs
     */
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        ObjectMapper mapper = new ObjectMapper();
        if(requestMethod.equals("DELETE")) {
            Map<String, String> params = HttpUtils.getQueryParams(exchange);
            String key = params.get("key");
            if(key != null) {
                boolean exists = dataStore.keySet().contains(key);
                if(exists) {
                    dataStore.remove(key);
                    ResponseMessage success = new ResponseMessage("Success", key, "Successfully deleted");
                    String message = mapper.writeValueAsString(success);
                    HttpUtils.sendResponse(exchange, 200, message);
                }else{
                    ResponseMessage valueErr = new ResponseMessage("Failed", key, "This key does not exist");
                    String message = mapper.writeValueAsString(valueErr);
                    HttpUtils.sendResponse(exchange, 404, message);
                }
            }else{
                ResponseMessage valueErr = new ResponseMessage("Failed", "NULL", "Must pass a query parameter key");
                String message = mapper.writeValueAsString(valueErr);
                HttpUtils.sendResponse(exchange, 404, message);
            }
        }
    }
}
