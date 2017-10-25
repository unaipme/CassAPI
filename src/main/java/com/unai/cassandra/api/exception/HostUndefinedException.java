package com.unai.cassandra.api.exception;

public class HostUndefinedException extends RuntimeException {

    public HostUndefinedException() {
        super("You must pass the hosts as arguments");
    }

}
