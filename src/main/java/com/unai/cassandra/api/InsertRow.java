package com.unai.cassandra.api;

import com.sun.corba.se.impl.io.TypeMismatchException;
import com.unai.cassandra.api.exception.ColumnUndefinedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class InsertRow {

    private Logger log = LoggerFactory.getLogger(InsertRow.class);

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
            throw new TypeMismatchException(String.format("Expected type %s", columns.get(tempColumn)));
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
        client.insertInto_internal(tableName, values);
    }

}
