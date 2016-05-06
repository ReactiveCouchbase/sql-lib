package org.reactivecouchbase.sql;

public class Pair {

    public final String key;

    public final Object value;

    Pair(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public final String toString() {
        return "Pair (key: " + key + ", value: " + value + ")";
    }

}