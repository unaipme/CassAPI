package com.unai.cassandra.api;

import com.unai.cassandra.api.data.Table;
import com.unai.cassandra.api.exception.LogicalClauseUndefinedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.unai.cassandra.api.DataType.*;

public class UpdateRow {

    private Logger log = LoggerFactory.getLogger(UpdateRow.class);

    private CassandraClient client;
    private Table table;
    private Map<String, UpdateColumn> updates;
    private List<String> conditions;

    private String nextClause = "";
    private String tempColumn = null;

    UpdateRow(String tableName, CassandraClient client) {
        this.client = client;
        this.table = client.describeTable(tableName);
        this.updates = new HashMap<>();
        this.conditions = new ArrayList<>();
    }

    public ValueAssigner set(String colName) {
        if (table.getColumnType(colName).equals(COUNTER))
            throw new InputMismatchException("Use method increment for counters.");
        this.tempColumn = colName;
        return new ValueAssigner(this);
    }

    public Incrementer increment(String colName) {
        if (!table.getColumnType(colName).equals(COUNTER))
            throw new InputMismatchException("It's not possible to increment anything other than counters");
        this.tempColumn = colName;
        return new Incrementer(this);
    }

    public Where where(String colName) {
        if ("".equals(nextClause) && !conditions.isEmpty()) {
            throw new LogicalClauseUndefinedException();
        }
        this.tempColumn = colName;
        return new Where(this);
    }

    public UpdateRow and() {
        this.nextClause = " AND ";
        log.info("Next clause is set to AND");
        return this;
    }

    public UpdateRow or() {
        this.nextClause = " OR ";
        log.info("Next clause is set to OR");
        return this;
    }

    private UpdateRow to(Object o, boolean increment) {
        updates.put(tempColumn, new UpdateColumn(o, increment));
        return this;
    }

    private UpdateRow where(String comparator, Object o) {
        String nextCondition = String.format("%s %s %s %s", nextClause, tempColumn, comparator, o.toString());
        conditions.add(nextCondition);
        log.info("Condition added: {}", nextCondition);
        this.tempColumn = null;
        return this;
    }

    public void commit() {
        client.update_interal(this);
    }

    private String getTempColumn() {
        return this.tempColumn;
    }

    Table getTable() {
        return this.table;
    }

    String getTableName() {
        return table.getTableName();
    }

    Map<String, UpdateColumn> getUpdates() {
        return this.updates;
    }

    List<String> getConditions() {
        return this.conditions;
    }

    public static class ValueAssigner {

        private UpdateRow update;

        private ValueAssigner(UpdateRow update) {
            this.update = update;
        }

        public UpdateRow to(Object o) {
            return update.to(o, false);
        }

        public UpdateRow to(int i) {
            return update.to(Integer.valueOf(i), false);
        }

        public UpdateRow to(double d) {
            return update.to(Double.valueOf(d), false);
        }

    }

    public static class Incrementer {

        private UpdateRow update;

        Incrementer(UpdateRow update) {
            this.update = update;
        }

        public UpdateRow by(int i) {
            if (i < 0) return update.to(String.format("%s - %d", update.getTempColumn(), -i), true);
            else return update.to(String.format("%s + %d", update.getTempColumn(), i), true);
        }

    }

    public static class Where {

        private UpdateRow update;

        private Where(UpdateRow update) {
            this.update = update;
        }

        private String treatString(String s) {
            return "'" + s.replaceAll("'", "''") + "'";
        }

        private UpdateRow end(String comparator, Object o) {
            if (TEXT.javaClass().isInstance(o)) return update.where(comparator, treatString(o.toString()));
            else return update.where(comparator, o);
        }

        public UpdateRow is(Object o) {
            return end("=", o);
        }

        public UpdateRow is(int i) {
            return is(Integer.valueOf(i));
        }

        public UpdateRow is(double d) {
            return is(Double.valueOf(d));
        }

        public UpdateRow is(boolean b) {
            return is(Boolean.valueOf(b));
        }

        /*
        public UpdateRow isNot(Object o) {
            return end("!=", o);
        }

        public UpdateRow isNot(int i) {
            return isNot(Integer.valueOf(i));
        }

        public UpdateRow isNot(double d) {
            return isNot(Double.valueOf(d));
        }

        public UpdateRow isNot(boolean b) {
            return isNot(Boolean.valueOf(b));
        }
        */

        public UpdateRow greaterThan(Object o) {
            return end(">", o);
        }

        public UpdateRow greaterThan(int i) {
            return greaterThan(Integer.valueOf(i));
        }

        public UpdateRow greaterThan(double d) {
            return greaterThan(Double.valueOf(d));
        }

        public UpdateRow greaterThan(boolean b) {
            return greaterThan(Boolean.valueOf(b));
        }
        public UpdateRow greaterThanOrEquals(Object o) {
            return end(">=", o);
        }

        public UpdateRow greaterThanOrEquals(int i) {
            return greaterThanOrEquals(Integer.valueOf(i));
        }

        public UpdateRow greaterThanOrEquals(double d) {
            return greaterThanOrEquals(Double.valueOf(d));
        }

        public UpdateRow greaterThanOrEquals(boolean b) {
            return greaterThanOrEquals(Boolean.valueOf(b));
        }

        public UpdateRow lessThan(Object o) {
            return end("<", o);
        }

        public UpdateRow lessThan(int i) {
            return lessThan(Integer.valueOf(i));
        }

        public UpdateRow lessThan(double d) {
            return lessThan(Double.valueOf(d));
        }

        public UpdateRow lessThan(boolean b) {
            return lessThan(Boolean.valueOf(b));
        }
        public UpdateRow lessThanOrEquals(Object o) {
            return end("<=", o);
        }

        public UpdateRow lessThanOrEquals(int i) {
            return lessThanOrEquals(Integer.valueOf(i));
        }

        public UpdateRow lessThanOrEquals(double d) {
            return lessThanOrEquals(Double.valueOf(d));
        }

        public UpdateRow lessThanOrEquals(boolean b) {
            return lessThanOrEquals(Boolean.valueOf(b));
        }
    }

}
