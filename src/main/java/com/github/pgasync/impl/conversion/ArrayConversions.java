package com.github.pgasync.impl.conversion;

import com.github.pgasync.impl.Oid;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;

enum ArrayConversions  {
    ;

    protected static Oid getElementOid(final Oid oid) {
        try {
            return Oid.valueOf(oid.name().replaceFirst("_ARRAY", ""));
        } catch (IllegalArgumentException e) {
            return Oid.UNSPECIFIED;
        }
    }

    protected static Class getElementType(Class arrayType) {
        while(arrayType.getComponentType() != null) {
            arrayType = arrayType.getComponentType();
        }
        return arrayType;
    }

    @SuppressWarnings("unchecked")
    public static <TArray> TArray toArray(
        Class<TArray> arrayType, Oid oid, byte[] value, BiFunction<Oid, byte[], Object> parse) {

        Oid elementOid = getElementOid(oid);
        return (TArray)buildArray(
            getElementType(arrayType), buildArrayList(value), 0, -1, (s) -> parse.apply(elementOid, s));
    }

    protected static Object[] buildArray(
        Class elementType, PgArrayList input, int index, int count, Function<byte[], Object> parse) {

        if (count < 0)
            count = input.size();

        int dimensionCount = input.dimensionsCount;
        int[] dimensions = new int[dimensionCount];
        for (int i = 0; i < dimensionCount; i++) {
            dimensions[i] = (i == 0 ? count : 0);
        }

        Object[] ret = (Object[])java.lang.reflect.Array.newInstance(elementType, dimensions);

        for (int length = 0; length < count; length++) {
            Object o = input.get(index++);
            if (o != null) {
                if (1 < dimensionCount) {
                    o = buildArray(elementType, (PgArrayList) o, 0, -1,  parse);
                } else {
                    o = parse.apply(((String) o).getBytes(UTF_8));
                }
            }
            ret[length] = o;
        }
        return ret;
    }

    // TODO: split and simplify
    @SuppressWarnings("unchecked")
    protected static PgArrayList buildArrayList(byte[] byteValue) {
        PgArrayList arrayList = new PgArrayList();

        char delim = ',';

        if (byteValue != null) {
            char[] chars = new String(byteValue).toCharArray();
            StringBuffer buffer = null;
            boolean insideString = false;
            boolean wasInsideString = false; // needed for checking if NULL
            // value occurred
            List dims = new ArrayList(); // array dimension arrays
            PgArrayList curArray = arrayList; // currently processed array

            // Starting with 8.0 non-standard (beginning index
            // isn't 1) bounds the dimensions are returned in the
            // data formatted like so "[0:3]={0,1,2,3,4}".
            // Older versions simply do not return the bounds.
            //
            // Right now we ignore these bounds, but we could
            // consider allowing these index values to be used
            // even though the JDBC spec says 1 is the first
            // index. I'm not sure what a client would like
            // to see, so we just retain the old behavior.
            int startOffset = 0;

            if (chars[0] == '[') {
                while (chars[startOffset] != '=') {
                    startOffset++;
                }
                startOffset++; // skip =
            }

            for (int i = startOffset; i < chars.length; i++) {
                // escape character that we need to skip
                if (chars[i] == '\\')
                    i++;

                    // subarray start
                else if (!insideString && chars[i] == '{') {
                    if (dims.size() == 0) {
                        dims.add(arrayList);
                    } else {
                        PgArrayList a = new PgArrayList();
                        PgArrayList p = ((PgArrayList) dims.get(dims.size() - 1));
                        p.add(a);
                        dims.add(a);
                    }
                    curArray = (PgArrayList) dims.get(dims.size() - 1);

                    // number of dimensions
                    for (int t = i + 1; t < chars.length; t++) {
                        if (Character.isWhitespace(chars[t]))
                            continue;
                        else if (chars[t] == '{')
                            curArray.dimensionsCount++;
                        else break;
                    }

                    buffer = new StringBuffer();
                    continue;
                }
                // quoted element
                else if (chars[i] == '"') {
                    insideString = !insideString;
                    wasInsideString = true;
                    continue;
                }
                // white space
                else if (!insideString && Character.isWhitespace(chars[i])) {
                    continue;
                }
                // array end or element end
                else if ((!insideString && (chars[i] == delim || chars[i] == '}')) || i == chars.length - 1) {
                    // when character that is a part of array element
                    if (chars[i] != '"' && chars[i] != '}' && chars[i] != delim && buffer != null) {
                        buffer.append(chars[i]);
                    }

                    String b = buffer == null ? null : buffer.toString();

                    // add element to current array
                    if (b != null && (b.length() > 0 || wasInsideString)) {
                        curArray.add(!wasInsideString && b.equals("NULL") ? null : b);
                    }

                    wasInsideString = false;
                    buffer = new StringBuffer();

                    // when end of an array
                    if (chars[i] == '}') {
                        dims.remove(dims.size() - 1);

                        // when multi-dimension
                        if (dims.size() > 0) {
                            curArray = (PgArrayList) dims.get(dims.size() - 1);
                        }

                        buffer = null;
                    }
                    continue;
                }

                if (buffer != null)
                    buffer.append(chars[i]);
            }
        }
        return arrayList;
    }

    public static byte[] fromArray(final Object[] elements) {
        StringBuffer sb = new StringBuffer();
        appendArray(sb, elements);
        return sb.toString().getBytes(UTF_8);
    }

    protected static void appendArray(final StringBuffer sb, final Object elements) {
        sb.append('{');

        int nElements = java.lang.reflect.Array.getLength(elements);
        for (int i = 0; i < nElements; i++) {
            if (i > 0) {
                sb.append(',');
            }

            Object o = java.lang.reflect.Array.get(elements, i);
            if (o == null) {
                sb.append("NULL");
            } else if (o.getClass().isArray()) {
                appendArray(sb, o);
            } else {
                String s = o.toString();
                escapeArrayElement(sb, s);
            }
        }
        sb.append('}');
    }

    protected static void escapeArrayElement(final StringBuffer b, final String s) {
        b.append('"');
        for (int j = 0; j < s.length(); j++) {
            char c = s.charAt(j);
            if (c == '"' || c == '\\') {
                b.append('\\');
            }

            b.append(c);
        }
        b.append('"');
    }

    private static class PgArrayList extends ArrayList {
        private static final long serialVersionUID = 2052783752654562677L;

        int dimensionsCount = 1;
    }
}