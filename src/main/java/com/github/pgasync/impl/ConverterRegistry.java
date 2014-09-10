package com.github.pgasync.impl;

import com.github.pgasync.Converter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Antti Laisi
 */
class ConverterRegistry {

    final Map<Class<?>,Converter<?>> objectToBytes = new HashMap<>();
    final Map<Oid,Converter> byteToObjects = new HashMap<>();

    ConverterRegistry(List<Converter<?>> converters) {
        converters.forEach(c -> objectToBytes.put(c.getType(), c));
    }

    @SuppressWarnings("unchecked")
    byte[] toBytes(Object o) {
        Converter converter = objectToBytes.get(o.getClass());
        if(converter == null) {
            throw new IllegalArgumentException("Unknown conversion target: " + o.getClass());
        }
        return converter.toBackend(o);
    }

    Object toObject(Oid oid, byte[] b) {
        return byteToObjects.get(oid).toClient(new Converter.ColumnData(oid, b));
    }

    @SuppressWarnings("unchecked")
    <T> T toType(Class<T> type, Oid oid, byte[] b) {
        Converter<T> converter = (Converter<T>) objectToBytes.get(type);
        if(converter == null) {
            throw new IllegalArgumentException("Unknown conversion target: " + type);
        }
        return converter.toClient(new Converter.ColumnData(oid, b));
    }
}
