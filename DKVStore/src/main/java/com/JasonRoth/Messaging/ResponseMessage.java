package com.JasonRoth.Messaging;

public class ResponseMessage {
    private String status;
    private String key;

    public ResponseMessage(){}

    public ResponseMessage(String status, String key) {
        this.status = status;
        this.key = key;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
