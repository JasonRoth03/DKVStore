package com.JasonRoth.util;

public class ResponseMessage {
    private String status;
    private String message;
    private String key;

    public ResponseMessage(){}

    public ResponseMessage(String status, String key, String message) {
        this.status = status;
        this.key = key;
        this.message = message;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
