package com.unai.cassandra;

import com.unai.cassandra.api.CassandraClient;

public class CassandraBulkWrite {

    public static void main(String [] args) {
        try (CassandraClient client = new CassandraClient(args)) {
            client.createKeyspace("ks_prueba");
            client.useKeyspace("ks_prueba");
            client.createTable("tabla_prueba").ifNotExists()
                    .withIntegerColumn("id").whichIsPartitionKey()
                    .withStringColumn("name")
                    .withBooleanColumn("isStudent")
                    .withDoubleColumn("salary")
                    .save();
           client.insertInto("tabla_prueba")
                   .forColumn("id").value(1)
                   .forColumn("name").value("Unai")
                   .save();
           client.insertInto("tabla_prueba")
                   .forColumn("id").value(2)
                   .forColumn("name").value("Iratxe")
                   .save();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
