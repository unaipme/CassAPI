package com.unai.cassandra.api;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.unai.cassandra.api.data.Table;
import com.unai.cassandra.api.exception.HostUndefinedException;
import com.unai.cassandra.api.exception.KeyspaceUndefinedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
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
        Locale.setDefault(Locale.ENGLISH);
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
        if (!cluster.getMetadata().getKeyspaces().stream().anyMatch(k -> k.getName().equals(name)))
            throw new KeyspaceUndefinedException(name);
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

    public Table describeTable(String tableName) {
        if (currentKS == null) throw new KeyspaceUndefinedException();
        log.info("Looking for description of table {}", tableName);
        Table table = Table.load(cluster, tableName, currentKS);
        log.info("Done");
        return table;
    }

    public DropTable dropTable(String tableName) {
        return new DropTable(tableName, this);
    }

    public UpdateRow update(String tableName) {
        return new UpdateRow(tableName, this);
    }

    void createTable_internal(CreateTable ct) {
        log.info("Creating table {} with columns:", ct.getTableName());
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        if (ct.isIfNotExists()) sb.append("IF NOT EXISTS ");
        sb.append(String.format("%s.%s (", currentKS, ct.getTableName()));
        ct.getColumns().forEach((k, v) -> {
            log.info("\t {} {}", k, v.type().trim());
            sb.append(String.format("%s %s,", k, v.type()));
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
        log.info("Inserting into table {}.{}", currentKS, i.getTableName());
        StringBuilder sb = new StringBuilder(String.format("INSERT INTO %s.%s ", currentKS, i.getTableName()));
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

    void dropTable_internal(DropTable d) {
        log.info("Dropping table {}", d.getTableName());
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (d.isIfExists()) sb.append("IF EXISTS ");
        sb.append(String.format("%s.%s", currentKS, d.getTableName()));
        log.debug(sb.toString());
        session.execute(sb.toString());
        log.info("The table has been dropped");
    }

    void update_interal(UpdateRow u) {
        log.info("Updating row from table {}.{}", currentKS, u.getTableName());
        StringBuilder sb = new StringBuilder(String.format("UPDATE %s.%s SET ", currentKS, u.getTableName()));
        sb.append(u.getUpdates()
                .entrySet()
                .stream()
                .map((e) -> String.format("%s=%s", e.getKey(), e.getValue().toString()))
                .collect(Collectors.joining(",")));
        if (!u.getConditions().isEmpty()) sb.append(" WHERE ");
        sb.append(u.getConditions().stream().collect(Collectors.joining(" ")));
        log.debug(sb.toString());
        session.execute(sb.toString());
        log.info("Done");
    }

    Session getSession() {
        return this.session;
    }

    Cluster getCluster() {
        return this.cluster;
    }

    @Override
    public void close() throws Exception {
        session.close();
        cluster.close();
    }

}
