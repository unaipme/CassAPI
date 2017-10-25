package com.unai.cassandra.api.exception;

public class LogicalClauseUndefinedException extends RuntimeException {

    public LogicalClauseUndefinedException() {
        super("For chaining conditions, a logical clause must be defined using or(), and() methods");
    }

}
