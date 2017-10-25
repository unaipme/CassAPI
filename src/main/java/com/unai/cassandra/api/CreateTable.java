package com.unai.cassandra.api;

import com.unai.cassandra.api.exception.AlreadyKeyException;
import com.unai.cassandra.api.exception.ColumnUndefinedException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CreateTable {

    private CassandraClient client;
    private String tableName;
    private Map<String, String> columns;
    private Set<String> partitionKeys;
    private Set<String> clusteringKeys;
    private boolean ifNotExists = false;

    private String lastColumn = null;

    private final static String BOOLEAN = " BOOLEAN ";
    private final static String BIGINT = " BIGINT ";
    private final static String DOUBLE = " DOUBLE ";
    private final static String STRING = " TEXT ";

    CreateTable(String name, CassandraClient client) {
        this.tableName = name;
        this.client = client;
        this.columns = new HashMap<>();
        this.partitionKeys = new HashSet<>();
        this.clusteringKeys = new HashSet<>();
    }

    public CreateTable ifNotExists() {
        this.ifNotExists = true;
        return this;
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

    public CreateTable whichIsPartitionKey() {
        if (lastColumn == null) throw new ColumnUndefinedException();
        return withPartitionKey(this.lastColumn);
    }

    public CreateTable withPartitionKey(String name) {
        if (columns.get(name) == null) throw new ColumnUndefinedException(name);
        if (isAlreadyKey(name)) throw new AlreadyKeyException();
        partitionKeys.add(name);
        return this;
    }

    public CreateTable withPartitionKeys(String... names) {
        for (String name : names) withPartitionKey(name);
        return this;
    }

    public CreateTable whichIsClusteringKey() {
        if (lastColumn == null) throw new ColumnUndefinedException();
        return withClusteringKey(lastColumn);
    }

    public CreateTable withClusteringKey(String name) {
        if (columns.get(name) == null) throw new ColumnUndefinedException(name);
        if (isAlreadyKey(name)) throw new AlreadyKeyException();
        clusteringKeys.add(name);
        return this;
    }

    public CreateTable withClusteringKeys(String... names) {
        for (String name : names) withClusteringKey(name);
        return this;
    }

    private boolean isAlreadyKey(String name) {
        return partitionKeys.contains(name) || clusteringKeys.contains(name);
    }

    public void save() {
        client.createTable_internal(this);
    }

    String getTableName() {
        return tableName;
    }

    Map<String, String> getColumns() {
        return columns;
    }

    Set<String> getPartitionKeys() {
        return partitionKeys;
    }

    Set<String> getClusteringKeys() {
        return clusteringKeys;
    }

    boolean isIfNotExists() {
        return ifNotExists;
    }
}
