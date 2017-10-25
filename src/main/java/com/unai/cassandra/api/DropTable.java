package com.unai.cassandra.api;

public class DropTable {

    private CassandraClient client;
    private String tableName;

    private boolean ifExists = false;

    DropTable(String tableName, CassandraClient client) {
        this.client = client;
        this.tableName = tableName;
    }

    public DropTable ifExists() {
        this.ifExists = true;
        return this;
    }

    public void commit() {
        client.dropTable_internal(this);
    }

    String getTableName() {
        return this.tableName;
    }

    boolean isIfExists() {
        return this.ifExists;
    }

}
