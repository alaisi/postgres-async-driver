package com.github.pgasync.impl.conversion;

import com.github.pgasync.impl.Oid;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

// TODO: change internal value format from byte[] to PgValue(TEXT|BINARY)
@SuppressWarnings({"unchecked","rawtypes"})
class ArrayConversions  {

    public static byte[] fromArray(final Object elements, final Function<Object,byte[]> printFn) {
        return appendArray(new StringBuilder(), elements, printFn).toString().getBytes(UTF_8);
    }

    static StringBuilder appendArray(StringBuilder sb, final Object elements, final Function<Object,byte[]> printFn) {
        sb.append('{');

        int nElements = Array.getLength(elements);
        for (int i = 0; i < nElements; i++) {
            if (i > 0) {
                sb.append(',');
            }

            Object o = Array.get(elements, i);
            if (o == null) {
                sb.append("NULL");
            } else if (o.getClass().isArray()) {
                sb = appendArray(sb, o, printFn);
            } else {
                sb = appendEscaped(sb, new String(printFn.apply(o), UTF_8));
            }
        }

        return sb.append('}');
    }

    static StringBuilder appendEscaped(final StringBuilder b, final String s) {
        b.append('"');
        for (int j = 0; j < s.length(); j++) {
            char c = s.charAt(j);
            if (c == '"' || c == '\\') {
                b.append('\\');
            }

            b.append(c);
        }
        return b.append('"');
    }

    public static <T> T toArray(Class<T> type, Oid oid, byte[] value, BiFunction<Oid,byte[],Object> parse) {
        Class elementType = getElementType(type);
        if(elementType.isPrimitive()) {
            throw new IllegalArgumentException("Primitive arrays are not supported due to possible NULL values");
        }

        if (value == null) {
            return null;
        }

        char[] text = new String(value, UTF_8).toCharArray();
        List<List<Object>> holder = new ArrayList<>(1);

        if(readArray(text, skipBounds(text, 0), (List) holder) != text.length) {
            throw new IllegalStateException("Failed to read array");
        }

        return (T) toNestedArrays(holder.get(0), elementType, getElementOid(oid), parse);
    }

    static int skipBounds(final char[] text, final int start) {
        if(text[start] != '[') {
            return start;
        }
        for(int end = start + 1;;) {
            if(text[end++] == '=') {
                return end;
            }
        }
    }

    static int readArray(final char[] text, final int start, List<Object> result) {
        List<Object> values = new ArrayList<>();
        for(int i = start + 1;;) {
            final char c = text[i];
            if(c == '}') {
                result.add(values);
                return i + 1;
            } else if(c == ',' || Character.isWhitespace(c)) {
                i++;
            } else if(c == '"') {
                i = readString(text, i, values);
            } else if(c == '{') {
                i = readArray(text, i, values);
            } else if (c == 'N' && text.length > i + 4 &&
                    text[i+1] == 'U' && text[i+2] == 'L' && text[i+3] == 'L' &&
                    (text[i+4] == ',' || text[i+4] == '}' || Character.isWhitespace(text[i+4]))) {
                i = readNull(i, values);
            } else {
                i = readValue(text, i, values);
            }
        }
    }

    static int readValue(final char[] text, final int start, List<Object> result) {
        StringBuilder str = new StringBuilder();
        for(int i = start;; i++) {
            char c = text[i];
            if(c == ',' || c == '}' || Character.isWhitespace(c)) {
                result.add(str.toString());
                return i;
            }
            str.append(c);
        }
    }

    static int readNull(final int i, final List<Object> result) {
        result.add(null);
        return i + 4;
    }

    static int readString(final char[] text, final int start, final List<Object> result) {
        StringBuilder str = new StringBuilder();
        for(int i = start + 1;;) {
            char c = text[i++];
            if(c == '"') {
                result.add(str.toString());
                return i;
            }
            if(c == '\\') {
                c = text[i++];
            }
            str.append(c);
        }
    }

    static Oid getElementOid(final Oid oid) {
        try {
            return Oid.valueOf(oid.name().replaceFirst("_ARRAY", ""));
        } catch (IllegalArgumentException e) {
            return Oid.UNSPECIFIED;
        }
    }

    static Class getElementType(Class arrayType) {
        while(arrayType.getComponentType() != null) {
            arrayType = arrayType.getComponentType();
        }
        return arrayType;
    }

    static <T> T[] toNestedArrays(List<Object> result, Class<?> type, Oid oid, BiFunction<Oid, byte[], Object> parse) {
        Object[] arr = (Object[]) Array.newInstance(type, getNestedDimensions(result));
        for(int i = 0; i < result.size(); i++) {
            Object elem = result.get(i);
            if(elem == null) {
                arr[i] = null;
            } else if(elem.getClass().equals(String.class)) {
                arr[i] = parse.apply(oid, ((String) elem).getBytes(UTF_8));
            } else {
                arr[i] = toNestedArrays((List<Object>) elem, type, oid, parse);
            }
        }
        return (T[]) arr;
    }

    static int[] getNestedDimensions(List<Object> result) {
        if(result.isEmpty()) {
            return new int[]{0};
        }
        if(!(result.get(0) instanceof List)) {
            return new int[]{ result.size() };
        }

        List<Integer> dimensions = new ArrayList<>();
        dimensions.add(result.size());

        Object value = result.get(0);
        while(value instanceof List) {
            List nested = (List) value;
            dimensions.add(nested.size());
            value = nested.isEmpty() ? null : nested.get(0);
        }

        return toIntArray(dimensions);
    }

    static int[] toIntArray(List<Integer> list) {
        int[] arr = new int[list.size()];
        for(int i = 0; i < arr.length; i++) {
            arr[i] = list.get(i);
        }
        return arr;
    }
}
