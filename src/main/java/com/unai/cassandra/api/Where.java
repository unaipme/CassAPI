package com.unai.cassandra.api;

public abstract class Where<T> {

    private String clause = "";

    private T caller;

    Where(T caller) {
        this.caller = caller;
    }

    protected T getCaller() {
        return this.caller;
    }

    public String getClause() {
        return this.clause;
    }

    protected void setClause(String clause) {
        this.clause = clause;
    }

    protected String treatString(String s) {
        return "'" + s.replaceAll("'", "''") + "'";
    }

    protected abstract WhereClause<? extends Where<T>, T> end(String comparator, Object... o);

    public WhereClause<? extends Where<T>, T> is(Object o) {
        return end("=", o);
    }

    public WhereClause<? extends Where<T>, T> is(int i) {
        return is(Integer.valueOf(i));
    }

    public WhereClause<? extends Where<T>, T> is(double d) {
        return is(Double.valueOf(d));
    }

    public WhereClause<? extends Where<T>, T> is(boolean b) {
        return is(Boolean.valueOf(b));
    }

    public WhereClause<? extends Where<T>, T> greaterThan(Object o) {
        return end(">", o);
    }

    public WhereClause<? extends Where<T>, T> greaterThan(int i) {
        return greaterThan(Integer.valueOf(i));
    }

    public WhereClause<? extends Where<T>, T> greaterThan(double d) {
        return greaterThan(Double.valueOf(d));
    }

    public WhereClause<? extends Where<T>, T> greaterThan(boolean b) {
        return greaterThan(Boolean.valueOf(b));
    }

    public WhereClause<? extends Where<T>, T> greaterThanOrEquals(Object o) {
        return end(">=", o);
    }

    public WhereClause<? extends Where<T>, T> greaterThanOrEquals(int i) {
        return greaterThanOrEquals(Integer.valueOf(i));
    }

    public WhereClause<? extends Where<T>, T> greaterThanOrEquals(double d) {
        return greaterThanOrEquals(Double.valueOf(d));
    }

    public WhereClause<? extends Where<T>, T> greaterThanOrEquals(boolean b) {
        return greaterThanOrEquals(Boolean.valueOf(b));
    }

    public WhereClause<? extends Where<T>, T> lessThan(Object o) {
        return end("<", o);
    }

    public WhereClause<? extends Where<T>, T> lessThan(int i) {
        return lessThan(Integer.valueOf(i));
    }

    public WhereClause<? extends Where<T>, T> lessThan(double d) {
        return lessThan(Double.valueOf(d));
    }

    public WhereClause<? extends Where<T>, T> lessThan(boolean b) {
        return lessThan(Boolean.valueOf(b));
    }

    public WhereClause<? extends Where<T>, T> lessThanOrEquals(Object o) {
        return end("<=", o);
    }

    public WhereClause<? extends Where<T>, T> lessThanOrEquals(int i) {
        return lessThanOrEquals(Integer.valueOf(i));
    }

    public WhereClause<? extends Where<T>, T> lessThanOrEquals(double d) {
        return lessThanOrEquals(Double.valueOf(d));
    }

    public WhereClause<? extends Where<T>, T> lessThanOrEquals(boolean b) {
        return lessThanOrEquals(Boolean.valueOf(b));
    }

    public WhereClause<? extends Where<T>, T> in(Object... o) {
        return end("IN", o);
    }

}
