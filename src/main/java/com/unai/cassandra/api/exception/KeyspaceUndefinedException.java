package com.unai.cassandra.api.exception;

public class KeyspaceUndefinedException extends RuntimeException {

    public KeyspaceUndefinedException() {
        super("No keyspace has been defined. Use useKeyspace method");
    }

    public KeyspaceUndefinedException(String s) {
        super(String.format("The keyspace %s does not exist", s));
    }

}
