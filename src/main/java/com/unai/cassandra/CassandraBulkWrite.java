package com.unai.cassandra;

import com.unai.cassandra.api.CassandraClient;

public class CassandraBulkWrite {

    public static void main(String [] args) {
        try (CassandraClient client = new CassandraClient(args)) {
            client.useKeyspace("ks_prueba");
            /*
            client.dropTable("tabla_prueba").ifExists().commit();
            client.createTable("tabla_prueba").ifNotExists()
                    .withIntegerColumn("id").whichIsPartitionKey()
                    .withStringColumn("name").whichIsClusteringKey()
                    .withBooleanColumn("isStudent").whichIsClusteringKey()
                    .withCounterColumn("salary")
                    .commit();
            */
            client.update("tabla_prueba")
                    .increment("salary").by(1)
                    .where("id").is(1).and()
                    .where("name").is("Unai").and()
                    .where("isStudent").is(true)
                    .commit();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
