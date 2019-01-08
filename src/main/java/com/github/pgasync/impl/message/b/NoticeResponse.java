package com.github.pgasync.impl.message.b;

import com.github.pgasync.impl.message.Message;

/**
 * @author Antti Laisi
 */
public class NoticeResponse implements Message {

    public static class Field {
        final byte type;
        final String value;

        private Field(byte type, String value) {
            this.type = type;
            this.value = value;
        }

        public byte getType() {
            return type;
        }

        public String getValue() {
            return value;
        }

        public static Field of(byte type, String value) {
            return new Field(type, value);
        }

        @Override
        public String toString() {
            return "{ type: " + type + "; value: \"" + value.replace("\"", "\"\"") + "\" }";
        }
    }

    final Field[] fields;

    public NoticeResponse(Field[] fields) {
        this.fields = fields;
    }

    public Field[] getFields() {
        return fields;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(fields[i].toString());
        }
        sb.append("]");
        return sb.toString();
    }
}
