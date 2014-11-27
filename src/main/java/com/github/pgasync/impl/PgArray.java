package com.github.pgasync.impl;

import com.github.pgasync.impl.conversion.DataConverter;

import java.util.ArrayList;
import java.util.List;

public class PgArray {
    protected final String string;
    protected final Object[] array;
    protected final Oid oid;
    protected final DataConverter converter;


    /* In the case where this object is being used to insert something into the database,
       this guy and toString/arrayToString are the only relevant methods
      */
    public PgArray(final Object[] array) {
        this(null, arrayToString(array), array, null);
    }

    public PgArray(final Oid oid, final String s, final DataConverter converter) {
        this(oid, s, null, converter);
    }

    public PgArray(final Oid oid, final String string, final Object[] array, final DataConverter converter) {
        this.oid = oid;
        this.string = string;
        this.array = array;
        this.converter = converter;
    }

    public String toString() {
        return string;
    }

    public Object[] getArray() {
        PgArrayList arrayList = buildArrayList();
        return buildArray(arrayList, 0, arrayList.size());
    }

    public static String arrayToString(final Object[] elements) {
        StringBuffer sb = new StringBuffer();
        appendArray(sb, elements);
        return sb.toString();
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

    protected PgArrayList buildArrayList() {
        PgArrayList arrayList = new PgArrayList();

        char delim = ',';

        if (string != null) {
            char[] chars = string.toCharArray();
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


    protected Object[] buildArray(PgArrayList input, int index, int count) {
        if (count < 0)
            count = input.size();

        Object[] ret;

        int dims = input.dimensionsCount;

        // dimensions length array (to be used with java.lang.reflect.Array.newInstance(Class<?>, int[]))
        int[] dimsLength = 1 < dims ? new int[dims] : null;
        if (1 < dims) {
            for (int i = 0; i < dims; i++) {
                dimsLength[i] = (i == 0 ? count : 0);
            }
        }
        int length = 0;

        if (1 < dims) {
            ret = (Object[]) java.lang.reflect.Array.newInstance(Object.class, dimsLength);
        } else {
            ret = new Object[count];
        }

        final Oid elementType = getElementType(oid);

        for (; 0 < count; count--) {
            Object o = input.get(index++);
            if (o == null) {
                o = null;
            } else if (1 < dims) {
                o = buildArray((PgArrayList) o, 0, -1);
            } else {
                o = converter.toObject(elementType, ((String) o).getBytes());
            }
            ret[length++] = o;
        }
        return ret;
    }

    protected static Oid getElementType(final Oid oid) {
        try {
            return Oid.valueOf(oid.name().replaceFirst("_ARRAY", ""));
        } catch (IllegalArgumentException _) {
            return Oid.UNSPECIFIED;
        }
    }

    private static class PgArrayList extends ArrayList {
        private static final long serialVersionUID = 2052783752654562677L;

        int dimensionsCount = 1;
    }
}