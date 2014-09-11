package com.github.pgasync;

import com.github.pgasync.impl.Oid;

/**
 * @author Antti Laisi.
 */
public interface Converter<T> {

    Class<T> type();

    byte[] from(T o);

    T to(Oid oid, byte[] value);

}
