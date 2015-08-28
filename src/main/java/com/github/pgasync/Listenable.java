package com.github.pgasync;

import java.util.function.Consumer;

/**
 * @author Antti Laisi
 */
public interface Listenable {

    void listen(String channel, Consumer<String> onNotification, Consumer<String> onListenStarted, Consumer<Throwable> onError);

    void unlisten(String channel, String unlistenToken, Runnable onListenStopped, Consumer<Throwable> onError);

}
