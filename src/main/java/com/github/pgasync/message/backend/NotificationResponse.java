package com.github.pgasync.message.backend;

import com.github.pgasync.message.Message;

/**
 * @author  Antti Laisi
 */
public class NotificationResponse implements Message {

    private final int backend;
    private final String channel;
    private final String payload;

    public NotificationResponse(int backend, String channel, String payload) {
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
