# CassAPI

(The name is hopefully temporary)

## What is this?

CassAPI is born as a way to personally start learning how Apache Cassandra and CQL in a practical way. As a Java enthusiast, I wanted to try interacting with the database using a Java driver. The most used option out there is the Datastax driver, which requires the user to write the CQL as a String, which I did not like at all.

CassAPI is built over Datastax driver and auto-generates the required CQL statement query. It uses method chaining to make readable code, and to make it easier to learn how to use it. Therefore, it's also very verbose, so choose your preference.

It is still in early development and I don't guarantee I'll ever finish it or make any kind of release.

## Example

```java
try (CassandraClient client = new CassandraClient(args)) {
    // Creating a new Keyspace named ks_test
    client.createKeyspace("ks_test");
    // Must then explicitly use it
    client.useKeyspace("ks_test");
    // Create a new table, with name table_test
    // If method ifNotExists() is not called and the table exists, an exception will occur
    client.createTable("table_test").ifNotExists()
        // Create integer column with name ID, which is a partition key
        .withIntegerColumn("id").whichIsPartitionKey()
        // Create string column with name name, which is a clustering key
        .withStringColumn("name").whichIsClusteringKey()
        // Create a column counter
        .withCounterColumn("counter")
        // Then run
        .execute();
        
    // As it's not permitted to INSERT INTO a table with counters, update the new table
    client.update("table_test")
        // Set counter to increment by 1, being 0 the default creation value
        .increment("counter").by(1)
        // Setting the WHERE conditions. id must be greater than 1 and name can be either "Unai" or "Iker"
        .where("id").greaterThan(1)
        .and("name").is("Unai")
        .or("name").is("Iker")
        .then().execute();
        
    // SELECT not yet developed
} catch (Exception e) {
    e.printStackTrace();
}
```
