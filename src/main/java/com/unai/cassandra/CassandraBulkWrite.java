package com.unai.cassandra;

import com.unai.cassandra.api.CassandraClient;

public class CassandraBulkWrite {

    public static void main(String [] args) {
        try (CassandraClient client = new CassandraClient(args)) {
            client.createKeyspace("ks_test");
            client.useKeyspace("ks_test");
            client.createTable("table_test").ifNotExists()
                    .withIntegerColumn("id").whichIsPartitionKey()
                    .withStringColumn("name").whichIsClusteringKey()
                    .withCounterColumn("counter")
                    .commit();
            client.update("table_test")
                    .increment("counter").by(1)
                    .where("id").is(1)
                    .and("name").is("Unai")
                    .then().execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
