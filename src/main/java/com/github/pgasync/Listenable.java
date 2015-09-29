package com.github.pgasync;

import rx.Observable;

/**
 * @author Antti Laisi
 */
public interface Listenable {

    Observable<String> listen(String channel);

}
