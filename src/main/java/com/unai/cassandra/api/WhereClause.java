package com.unai.cassandra.api;

public interface WhereClause<T extends Where<U>, U> {

    public T and(String colName);
    public T or(String colName);
    public U then();

}
