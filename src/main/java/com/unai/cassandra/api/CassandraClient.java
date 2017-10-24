package com.unai.cassandra.api;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Session;
import com.unai.cassandra.api.exception.KeyspaceUndefinedException;
import com.unai.cassandra.api.exception.TableUndefinedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        this.currentKS = name;
        log.info("Using keyspace {}", name);
        session.execute(String.format("USE %s", name));
    }

    public CreateTable createTable(String name) {
        if (currentKS == null) throw new KeyspaceUndefinedException();
        return new CreateTable(name, this);
    }

    public InsertRow insertInto(String tableName) {
        if (currentKS == null) throw new KeyspaceUndefinedException();
        return new InsertRow(tableName, this);
    }

    public Map<String, String> describeTable(String tableName) {
        if (currentKS == null) throw new KeyspaceUndefinedException();
        log.info("Looking for description of table {}", tableName);
        List<ColumnMetadata> cols = null;
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

    void createTable_internal(String name, Map<String, String> cols, Set<String> pks) {
        final StringBuilder sb = new StringBuilder(String.format("CREATE TABLE IF NOT EXISTS %s (", name));
        log.info("Creating table {} with columns:", name);
        cols.forEach((k, v) -> {
            log.info("\t {} {}", k, v.trim());
            sb.append(String.format("%s %s,", k, v));
        });
        sb.append(String.format("PRIMARY KEY (%s) );", pks.stream().collect(Collectors.joining(","))));
        log.info(sb.toString());
        session.execute(sb.toString());
        log.info("The table has been created");
    }

    void insertInto_internal(String tableName, Map<String, Object> values) {
        final StringBuilder sb = new StringBuilder(String.format("INSERT INTO %s ", tableName));
        log.info("Inserting into table {}", tableName);
        values.forEach((k, v) -> log.info("\t{} = {}", k, v));
        sb.append("(" + values.keySet().stream().collect(Collectors.joining(",")) + ")");
        sb.append(" VALUES (" + values.values()
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
