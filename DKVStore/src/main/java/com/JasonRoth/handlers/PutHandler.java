package com.JasonRoth.handlers;

import com.JasonRoth.util.HttpUtils;
import com.JasonRoth.util.KeyValue;
import com.JasonRoth.util.ResponseMessage;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.util.Map;

/**
 * Handles put requests for the key value store
 */
public class PutHandler implements HttpHandler {
    private Map<String, String> dataStore;

    public PutHandler(Map<String, String> dataStore) {
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
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        if(requestMethod.equals("POST") && contentType.equals("application/json")){
            //get the JSON body as a KeyValue object
            ObjectMapper mapper = new ObjectMapper();

            String requestBody = HttpUtils.readRequestBody(exchange);

            if(requestBody.isEmpty()){ //send error since no request body was provided
                ResponseMessage error = new ResponseMessage("Failed", "NULL", "Request body is empty");
                String message = mapper.writeValueAsString(error);
                HttpUtils.sendResponse(exchange, 404, message);
            }
            KeyValue kv = null;
            try{
                kv = mapper.readValue(requestBody, KeyValue.class);
            }catch (JsonMappingException jme){
                ResponseMessage error = new ResponseMessage("Failed", "NULL", "Failed to parse request body");
                String message = mapper.writeValueAsString(error);
                HttpUtils.sendResponse(exchange, 500, message);
            }

            dataStore.put(kv.getKey(), kv.getValue());

            ResponseMessage success = new ResponseMessage("Success", kv.getKey(), "Successfully mapped to: " + kv.getValue());
            String message = mapper.writeValueAsString(success);
            HttpUtils.sendResponse(exchange, 200, message);
        }
    }
}
