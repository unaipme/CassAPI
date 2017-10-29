package com.unai.cassandra.api;

import com.unai.cassandra.api.data.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.unai.cassandra.api.DataType.*;

public class UpdateRow {

    private Logger log = LoggerFactory.getLogger(UpdateRow.class);

    private CassandraClient client;
    private Table table;
    private Map<String, UpdateColumn> updates;
    private List<String> conditions;

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

    public UpdateWhere where(String colName) {
        this.tempColumn = colName;
        return new UpdateWhere(this);
    }

    private UpdateRow to(Object o, boolean increment) {
        updates.put(tempColumn, new UpdateColumn(o, increment));
        return this;
    }

    private UpdateRow where(String clause, String comparator, Object o) {
        String nextCondition = String.format("%s %s %s %s", tempColumn, comparator, o.toString(), clause);
        conditions.add(nextCondition);
        log.info("Condition added: {}", nextCondition);
        this.tempColumn = null;
        return this;
    }

    private void setTempColumn(String colName) {
        this.tempColumn = colName;
    }

    public void execute() {
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

    public static class UpdateWhere extends Where<UpdateRow> {

        private UpdateWhere(UpdateRow update) {
            super(update);
        }

        protected WhereClause<UpdateWhere, UpdateRow> end(String comparator, Object... o) {
            Object aux;
            if (o.length > 1 && comparator.trim().equals("IN"))
                aux = "(" + Arrays.asList(o)
                        .stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(",")) + ")";
            else if (TEXT.javaClass().isInstance(o[0])) aux = treatString(o[0].toString());
            else aux = o[0];
            return new WhereClause<UpdateWhere, UpdateRow>() {
                @Override
                public UpdateWhere and(String colName) {
                    getCaller().where(" AND ", comparator, aux).setTempColumn(colName);
                    return new UpdateWhere(getCaller());
                }

                @Override
                public UpdateWhere or(String colName) {
                    getCaller().where(" OR ", comparator, aux).setTempColumn(colName);
                    return new UpdateWhere(getCaller());
                }

                @Override
                public UpdateRow then() {
                    return getCaller().where("", comparator, aux);
                }
            };
        }

    }

}
