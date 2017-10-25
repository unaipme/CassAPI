package com.unai.cassandra.api;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.unai.cassandra.api.exception.HostUndefinedException;
import com.unai.cassandra.api.exception.KeyspaceUndefinedException;
import com.unai.cassandra.api.exception.TableUndefinedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.datastax.driver.core.Cluster.Builder;

public class CassandraClient implements AutoCloseable {

    private Logger log = LoggerFactory.getLogger(CassandraClient.class);

    private Cluster cluster;
    private Session session;

    private String currentKS = null;

    private final static String DEF_REPL_STRATEGY = "SimpleStrategy";
    private final static Integer DEF_REPL_FACTOR = 2;

    private final static String QRY_TMPL_CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS %s " +
            "WITH replication = {'class':'%s','replication_factor':%d};";

    public CassandraClient(String... addrs) {
        if (addrs.length == 0) throw new HostUndefinedException();
        Builder b = Cluster.builder();
        for (String a : addrs) b.addContactPoint(a);
        this.cluster = b.build();
        this.session = cluster.connect();
    }

    public void createKeyspace(String name) {
        createKeyspace(name, DEF_REPL_STRATEGY, DEF_REPL_FACTOR);
    }

    public void createKeyspace(String name, String replStrategy, Integer replFactor) {
        log.info("Creating keyspace {}, with replication = {class: {}, factor: {}}", name, replStrategy, replFactor);
        String query = String.format(QRY_TMPL_CREATE_KEYSPACE, name, replStrategy, replFactor);
        log.debug(query);
        session.execute(query);
        log.info("Done");
    }

    public void useKeyspace(String name) {
        if (name.equals(currentKS)) return;
        this.currentKS = name;
        log.info("Using keyspace {}", name);
    }

    public CreateTable createTable(String tableName) {
        if (currentKS == null) throw new KeyspaceUndefinedException();
        return new CreateTable(tableName, this);
    }

    public InsertRow insertInto(String tableName) {
        if (currentKS == null) throw new KeyspaceUndefinedException();
        return new InsertRow(tableName, this);
    }

    public Map<String, String> describeTable(String tableName) {
        if (currentKS == null) throw new KeyspaceUndefinedException();
        log.info("Looking for description of table {}", tableName);
        List<ColumnMetadata> cols;
        try {
            cols = cluster.getMetadata().getKeyspace(currentKS).getTable(tableName).getColumns();
        } catch (NullPointerException e) {
            throw new TableUndefinedException(tableName);
        }
        Map<String, String> table = new HashMap<>();
        cols.forEach(c -> {
            table.put(c.getName(), c.getType().getName().name());
            log.info("\tColumn {} of type {}", c.getName(), c.getType().getName().name());
        });
        log.info("Done");
        return table;
    }

    void createTable_internal(CreateTable ct) {
        log.info("Creating table {} with columns:", ct.getTableName());
       //StringBuilder sb = new StringBuilder(String.format("CREATE TABLE IF NOT EXISTS %s.%s (", currentKS, name));
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        if (ct.isIfNotExists()) sb.append("IF NOT EXISTS ");
        sb.append(String.format("%s.%s (", currentKS, ct.getTableName()));
        ct.getColumns().forEach((k, v) -> {
            log.info("\t {} {}", k, v.trim());
            sb.append(String.format("%s %s,", k, v));
        });
        sb.append(String.format("PRIMARY KEY ((%s)", ct.getPartitionKeys()
                .stream()
                .collect(Collectors.joining(","))));
        if (!ct.getClusteringKeys().isEmpty())
            sb.append(", " + ct.getClusteringKeys().stream().collect(Collectors.joining(",")));
        sb.append("));");
        log.debug(sb.toString());
        session.execute(sb.toString());
        log.info("The table has been created");
    }

    void insertInto_internal(InsertRow i) {
        StringBuilder sb = new StringBuilder(String.format("INSERT INTO %s.%s ", currentKS, i.getTableName()));
        log.info("Inserting into table {}.{}", currentKS, i.getTableName());
        i.getValues().forEach((k, v) -> log.info("\t{} = {}", k, v));
        sb.append("(" + i.getValues().keySet().stream().collect(Collectors.joining(",")) + ")");
        sb.append(" VALUES (" + i.getValues().values()
                .stream()
                .map(v -> (v instanceof String) ? "'" + v + "'" : v.toString())
                .collect(Collectors.joining(","))+ ")");
        log.debug(sb.toString());
        session.execute(sb.toString());
        log.info("The row has been successfully inserted");
    }

    @Override
    public void close() throws Exception {
        session.close();
        cluster.close();
    }

}
