package com.unai.cassandra.api.data;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.unai.cassandra.api.DataType;
import com.unai.cassandra.api.exception.ColumnUndefinedException;
import com.unai.cassandra.api.exception.TableUndefinedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Table {

    private Logger log = LoggerFactory.getLogger(Table.class);

    private String tableName;
    private String keyspace;
    private Map<String, DataType> columns;

    private Table(String tableName, String keyspace) {
        this.tableName = tableName;
        this.keyspace = keyspace;
        this.columns = new HashMap<>();
    }

    private void addColumn(ColumnMetadata cm) {
        log.info("\tColumn {} of type {}", cm.getName(), cm.getType().getName().name());
        columns.put(cm.getName(), DataType.valueOf(cm.getType().getName().name()));
    }

    public boolean columnExists(String colName) {
        return columns.containsKey(colName);
    }

    public DataType getColumnType(String colName) {
        if (!columnExists(colName)) throw new ColumnUndefinedException(colName);
        return columns.get(colName);
    }

    public String getTableName() {
        return this.tableName;
    }

    public Map<String, DataType> getColumns() {
        return columns;
    }

    public static Table load(Cluster cluster, String tableName, String keyspace) {
        Table table = new Table(tableName, keyspace);
        try {
            cluster.getMetadata()
                    .getKeyspace(keyspace)
                    .getTable(tableName)
                    .getColumns()
                    .forEach(cm -> table.addColumn(cm));
        } catch (NullPointerException e) {
            throw new TableUndefinedException(tableName);
        }
        return table;
    }

}
