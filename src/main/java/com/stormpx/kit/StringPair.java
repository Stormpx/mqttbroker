package com.stormpx.kit;

import java.util.Objects;

public class StringPair {
    private String key;
    private String value;


    public StringPair(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StringPair pair = (StringPair) o;
        return Objects.equals(key, pair.key) && Objects.equals(value, pair.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "StringPair{" + "key='" + key + '\'' + ", value='" + value + '\'' + '}';
    }
}
