package com.unai.cassandra.api.exception;

public class TableUndefinedException extends RuntimeException {

    public TableUndefinedException() {
        super("No table has been defined");
    }

    public TableUndefinedException(String s) {
        super(String.format("Table %s does not exist", s));
    }

}
