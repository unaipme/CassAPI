package com.unai.cassandra.api.exception;

public class ColumnUndefinedException extends RuntimeException {

    public ColumnUndefinedException() {
        super("A column must be defined first");
    }

    public ColumnUndefinedException(String s) {
        super(String.format("Column '%s' has not been defined", s));
    }

}
