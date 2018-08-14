package com.gfyulx.DI.schedule.util;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.logging.Logger;


public class DefaultMethodsSupport {

    private static final Logger LOG = Logger.getLogger(DefaultMethodsSupport.class.getName());

    // helper method for getAt and putAt
    /**
    protected static RangeInfo subListBorders(int size, Range range) {
        int from = normaliseIndex(DefaultTypeTransformation.intUnbox(range.getFrom()), size);
        int to = normaliseIndex(DefaultTypeTransformation.intUnbox(range.getTo()), size);
        boolean reverse = range.isReverse();
        if (from > to) {
            // support list[1..-1]
            int tmp = to;
            to = from;
            from = tmp;
            reverse = !reverse;
        }
        return new RangeInfo(from, to + 1, reverse);
    }
	**/
    // helper method for getAt and putAt
    /**
    protected static RangeInfo subListBorders(int size, EmptyRange range) {
        int from = normaliseIndex(DefaultTypeTransformation.intUnbox(range.getFrom()), size);
        return new RangeInfo(from, from, false);
    }
**/
    /**
     * This converts a possibly negative index to a real index into the array.
     *
     * @param i    the unnormalised index
     * @param size the array size
     * @return the normalised index
     */
    protected static int normaliseIndex(int i, int size) {
        int temp = i;
        if (i < 0) {
            i += size;
        }
        if (i < 0) {
            throw new ArrayIndexOutOfBoundsException("Negative array index [" + temp + "] too large for array size " + size);
        }
        return i;
    }

    /**
     * Close the Closeable. Logging a warning if any problems occur.
     *
     * @param c the thing to close
     */
    public static void closeWithWarning(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                LOG.warning("Caught exception during close(): " + e);
            }
        }
    }

    /**
     * Close the Closeable. Ignore any problems that might occur.
     *
     * @param c the thing to close
     */
    public static void closeQuietly(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                /* ignore */
            }
        }
    }

    protected static class RangeInfo {
        public final int from;
        public final int to;
        public final boolean reverse;

        public RangeInfo(int from, int to, boolean reverse) {
            this.from = from;
            this.to = to;
            this.reverse = reverse;
        }
    }

    protected static <T> List<T> createSimilarList(List<T> orig, int newCapacity) {
        if (orig instanceof LinkedList)
            return new LinkedList<T>();

        if (orig instanceof Stack)
            return new Stack<T>();

        if (orig instanceof Vector)
            return new Vector<T>();

        return new ArrayList<T>(newCapacity);
    }

    protected static <T> T[] createSimilarArray(T[] orig, int newCapacity) {
        Class<T> componentType = (Class<T>) orig.getClass().getComponentType();
        return (T[]) Array.newInstance(componentType, newCapacity);
    }
}
