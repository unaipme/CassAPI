package com.unai.cassandra.api.data;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.TableMetadata;
import com.unai.cassandra.api.DataType;
import com.unai.cassandra.api.exception.ColumnUndefinedException;
import com.unai.cassandra.api.exception.TableUndefinedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Table {

    private Logger log = LoggerFactory.getLogger(Table.class);

    private String tableName;
    private String keyspace;
    //private Map<String, DataType> columns;
    private List<Column> columns;

    private Table(String tableName, String keyspace) {
        this.tableName = tableName;
        this.keyspace = keyspace;
        this.columns = new ArrayList<>();
    }

    private void addColumn(TableMetadata meta, ColumnMetadata cm) {
        String name = cm.getName();
        log.info("\tColumn {} of type {}", name, cm.getType().getName().name());
        columns.add(Column.builder().withName(name)
                .withType(DataType.valueOf(cm.getType().getName().name()))
                .isPartitionKey(meta.getPartitionKey().stream().anyMatch(c -> c.getName().equals(name)))
                .isClusteringKey(meta.getClusteringColumns().stream().anyMatch(c -> c.getName().equals(name)))
                .build());
    }

    public boolean columnExists(String colName) {
        return columns.stream().anyMatch(c -> c.getName().equals(colName));
    }

    public DataType getColumnType(String colName) {
        if (!columnExists(colName)) throw new ColumnUndefinedException(colName);
        return columns.stream().filter(c -> c.getName().equals(colName)).findFirst().get().getType();
    }

    public String getTableName() {
        return this.tableName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public static Table load(Cluster cluster, String tableName, String keyspace) {
        Table table = new Table(tableName, keyspace);
        try {
            TableMetadata meta = cluster.getMetadata().getKeyspace(keyspace).getTable(tableName);
            meta.getColumns().forEach(cm -> table.addColumn(meta, cm));
        } catch (NullPointerException e) {
            throw new TableUndefinedException(tableName);
        }
        return table;
    }

}
