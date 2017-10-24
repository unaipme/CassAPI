package com.unai.cassandra.api;

public enum ClassCaster {
    BIGINT(Integer.class),
    TEXT(String.class),
    DOUBLE(Double.class),
    BOOLEAN(Boolean.class);

    private Class<?> clazz;

    ClassCaster(Class<?> clazz) {
        this.clazz = clazz;
    }

    public Class<?> javaClass() {
        return this.clazz;
    }

}
