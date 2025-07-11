package com.JasonRoth.Messaging;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Utility class for framing Inter-Node messages communicated via TCP
 */
public class PeerMessageFramer {

    //Maximum message size default is 4MB
    private static final int MAX_ALLOWED_MESSAGE_SIZE = 4 * 1024 * 1024;

    /**
     * Inner class to hold de-framed messages
     */
    public static class FramedMessage{
        public final byte messageType;
        public final byte[] payload;

        public FramedMessage(byte messageType, byte[] payload) {
            this.messageType = messageType;
            this.payload = payload;
        }


        /**
         * Gets the payload byte[] as a string
         * @return the payload as a string
         */
        public String getPayloadAsString(){
            return new String(payload, StandardCharsets.UTF_8);
        }
    }

    /**
     * Messages are framed as:
     * 4 bytes - length of payload in bytes
     * 1 byte - corresponds to a message type
     * n bytes - length of the payload
     * @param dis the data input stream where messages are being read
     * @return FramedMessage object representing the message that was read
     * @throws IOException
     */
    public static FramedMessage readNextMessage(DataInputStream dis) throws IOException {
        int length;
        try{
            length = dis.readInt();
        }catch (EOFException eof){
            throw new EOFException("Stream ended while trying to read the message length");
        }

        if(length <= 0){
            throw new IOException("Invalid message length, must be greater than 0 for message type");
        }

        if(length > MAX_ALLOWED_MESSAGE_SIZE){
            throw new IOException("Message length: " + length + " exceeds maximum allowed length: " + MAX_ALLOWED_MESSAGE_SIZE );
        }

        byte messageType = dis.readByte();
        int payloadLength = length - 1; //the length in bytes minus 1 byte used for a message type
        if(payloadLength < 0){
            throw new IOException("Invalid message length, payload length must not be a negative number");
        }
        byte[] payload = new byte[payloadLength];
        if(payloadLength > 0){
            dis.readFully(payload);
        }

        //if the payload length is 0, then nothing is read
        return new FramedMessage(messageType, payload);
    }

    /**
     * writes a message on the provided output stream as a length prefixed message and flushes the output
     * @param dos - the DataOutputStream for the connection
     * @param messageType - the message type as a byte
     * @param payload - the payload of the message as a byte array
     * @throws IOException if an i/o error occurs when trying to write to the stream
     */
    public static void writeMessage(DataOutputStream dos, byte messageType, byte[] payload) throws IOException {
        int payloadLength = (payload == null ? 0 : payload.length);
        int messageLength = payloadLength + 1;
        dos.writeInt(messageLength);
        dos.writeByte(messageType);
        if(payloadLength > 0){
            dos.write(payload);
        }
        //flush any buffered output bytes to the data stream
        dos.flush();
    }
}
