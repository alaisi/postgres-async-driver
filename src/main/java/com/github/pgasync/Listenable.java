package com.github.pgasync;

import io.reactivex.Observable;

/**
 * @author Antti Laisi
 */
public interface Listenable {

    Observable<String> listen(String channel);

}
