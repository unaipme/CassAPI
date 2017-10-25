package com.unai.cassandra.api;

import com.unai.cassandra.api.exception.ColumnUndefinedException;

import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Map;

public class InsertRow {

    private CassandraClient client;

    private Map<String, String> columns;
    private Map<String, Object> values;

    private String tableName;
    private String tempColumn = null;

    InsertRow(String tableName, CassandraClient client) {
        this.tableName = tableName;
        this.client = client;
        this.columns = client.describeTable(tableName);
        this.values = new HashMap<>();
    }

    public InsertRow forColumn(String colName) {
        if (!columns.containsKey(colName)) throw new ColumnUndefinedException(colName);
        this.tempColumn = colName;
        return this;
    }

    public InsertRow value(Object o) {
        if (tempColumn == null) throw new ColumnUndefinedException();
        if (!ClassCaster.valueOf(columns.get(tempColumn)).javaClass().isInstance(o))
            throw new InputMismatchException(String.format("Expected type %s", columns.get(tempColumn)));
        values.put(tempColumn, o);
        this.tempColumn = null;
        return this;
    }

    public InsertRow value(int i) {
        return value(Integer.valueOf(i));
    }

    public InsertRow value(double d) {
        return value(Double.valueOf(d));
    }

    public void save() {
        client.insertInto_internal(this);
    }

    String getTableName() {
        return this.tableName;
    }

    Map<String, Object> getValues() {
        return values;
    }

}
