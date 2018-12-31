package com.github.pgasync.impl.message;

/**
 * @author  Antti Laisi
 */
public class Notification implements Message {

    final int backend;
    final String channel;
    final String payload;

    public Notification(int backend, String channel, String payload) {
        this.backend = backend;
        this.channel = channel;
        this.payload = payload;
    }

    public String getChannel() {
        return channel;
    }

    public String getPayload() {
        return payload;
    }
}
