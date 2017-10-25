package com.unai.cassandra.api.exception;

public class AlreadyKeyException extends RuntimeException {

    public AlreadyKeyException() {
        super("The column is already defined to be a key.");
    }

}
