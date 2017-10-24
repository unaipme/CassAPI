package com.unai.cassandra.api;

import com.unai.cassandra.api.exception.ColumnUndefinedException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CreateTable {

    private CassandraClient client;
    private String tableName;
    private Map<String, String> columns;
    private Set<String> primaryKeys;

    private String lastColumn = null;

    private final static String BOOLEAN = " BOOLEAN ";
    private final static String BIGINT = " BIGINT ";
    private final static String DOUBLE = " DOUBLE ";
    private final static String STRING = " TEXT ";

    CreateTable(String name, CassandraClient client) {
        this.tableName = name;
        this.client = client;
        this.columns = new HashMap<>();
        this.primaryKeys = new HashSet<>();
    }

    public CreateTable withBooleanColumn(String name) {
        columns.put(name, BOOLEAN);
        this.lastColumn = name;
        return this;
    }

    public CreateTable withIntegerColumn(String name) {
        columns.put(name, BIGINT);
        this.lastColumn = name;
        return this;
    }

    public CreateTable withDoubleColumn(String name) {
        columns.put(name, DOUBLE);
        this.lastColumn = name;
        return this;
    }

    public CreateTable withStringColumn(String name) {
        columns.put(name, STRING);
        this.lastColumn = name;
        return this;
    }

    public CreateTable whichIsPrimaryKey() {
        if (lastColumn == null) throw new ColumnUndefinedException();
        withPrimaryKey(this.lastColumn);
        return this;
    }

    public CreateTable withPrimaryKey(String name) {
        if (columns.get(name) == null) throw new ColumnUndefinedException(name);
        primaryKeys.add(name);
        return this;
    }

    public CreateTable withPrimaryKeys(String... names) {
        for (String name : names) withPrimaryKey(name);
        return this;
    }

    public void save() {
        client.createTable_internal(tableName, columns, primaryKeys);
    }

}
