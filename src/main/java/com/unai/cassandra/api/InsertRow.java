package com.unai.cassandra.api;

import com.unai.cassandra.api.data.Table;
import com.unai.cassandra.api.exception.ColumnUndefinedException;
import com.unai.cassandra.api.exception.InsertWithCounterException;

import javax.naming.OperationNotSupportedException;
import java.util.HashMap;
import java.util.InputMismatchException;
import java.util.Map;

import static com.unai.cassandra.api.DataType.COUNTER;

public class InsertRow {

    private CassandraClient client;

    private Table table;
    private Map<String, Object> values;

    private String tableName;
    private String tempColumn = null;

    InsertRow(String tableName, CassandraClient client) {
        if (table.getColumns().stream().anyMatch(c -> c.getType().equals(COUNTER)))
            throw new InsertWithCounterException(tableName);
        this.tableName = tableName;
        this.client = client;
        this.table = client.describeTable(tableName);
        this.values = new HashMap<>();
    }

    public AcceptValue forColumn(String colName) {
        if (!table.columnExists(colName)) throw new ColumnUndefinedException(colName);
        this.tempColumn = colName;
        return new AcceptValue(this);
    }

    private InsertRow value(Object o) {
        DataType type = table.getColumnType(tempColumn);
        if (tempColumn == null) throw new ColumnUndefinedException();
        if (!type.javaClass().isInstance(o))
            throw new InputMismatchException(String.format("Expected type %s", type.type()));
        values.put(tempColumn, o);
        this.tempColumn = null;
        return this;
    }

    public void commit() {
        client.insertInto_internal(this);
    }

    String getTableName() {
        return this.tableName;
    }

    Map<String, Object> getValues() {
        return values;
    }

    public static class AcceptValue {

        private InsertRow insert;

        private AcceptValue(InsertRow insert) {
            this.insert = insert;
        }

        public InsertRow value(Object o) {
            return insert.value(o);
        }

        public InsertRow value(int i) {
            return value(Integer.valueOf(i));
        }

        public InsertRow value(double d) {
            return value(Double.valueOf(d));
        }

        public InsertRow value(boolean d) {
            return value(Boolean.valueOf(d));
        }

    }

}
