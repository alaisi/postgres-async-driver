package com.github.pgasync.impl.message.backend;

import com.github.pgasync.impl.message.Message;

/**
 * @author  Antti Laisi
 */
public class NotificationResponse implements Message {

    final int backend;
    final String channel;
    final String payload;

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
