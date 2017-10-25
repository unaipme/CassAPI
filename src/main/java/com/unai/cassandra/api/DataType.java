package com.unai.cassandra.api;

public enum DataType {
    BIGINT("BIGINT", Integer.class),
    TEXT("TEXT", String.class),
    DOUBLE("DOUBLE", Double.class),
    BOOLEAN("BOOLEAN", Boolean.class),
    COUNTER("COUNTER", Integer.class);

    private Class<?> clazz;
    private String name;

    DataType(String name, Class<?> clazz) {
        this.name = name;
        this.clazz = clazz;
    }

    public Class<?> javaClass() {
        return this.clazz;
    }

    public String type() {
        return " " + this.name + " ";
    }

}
