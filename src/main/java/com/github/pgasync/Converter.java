package com.github.pgasync;

import com.github.pgasync.impl.Oid;

/**
 * @author Antti Laisi.
 */
public interface Converter<T> {

    Class<T> getType();

    byte[] toBackend(T o);

    T toClient(ColumnData columnData);

    public static class ColumnData {
        public final Oid oid;
        public final byte[] data;
        public ColumnData(Oid oid, byte[] data) {
            this.oid = oid;
            this.data = data;
        }
    }
}
