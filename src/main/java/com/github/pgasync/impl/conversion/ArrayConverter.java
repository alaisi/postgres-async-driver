package com.github.pgasync.impl.conversion;

import com.github.pgasync.Converter;
import com.github.pgasync.impl.Oid;
import com.github.pgasync.impl.PgArray;

public class ArrayConverter implements Converter<PgArray> {
    protected final DataConverter converter;

    public ArrayConverter(DataConverter converter) {
        this.converter = converter;
    }

    @Override
    public Class<PgArray> type() {
        return PgArray.class;
    }

    @Override
    public byte[] from(final PgArray o) {
        return o.toString().getBytes();
    }

    @Override
    public PgArray to(Oid oid, byte[] value) {
        return value == null ? null : new PgArray(oid, new String(value), converter);
    }
}