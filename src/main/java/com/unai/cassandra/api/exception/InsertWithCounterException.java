package com.unai.cassandra.api.exception;

public class InsertWithCounterException extends RuntimeException {

    public InsertWithCounterException(String s) {
        super(String.format("You can't insert into table %s because it has a counter column", s));
    }

    public InsertWithCounterException() {
        super("You can't insert into a table with counter column");
    }

}
