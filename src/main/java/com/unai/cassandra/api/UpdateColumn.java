package com.unai.cassandra.api;

import static com.unai.cassandra.api.DataType.TEXT;

public class UpdateColumn {

    private String updateValue;

    UpdateColumn(Object o) {
        this(o, false);
    }

    UpdateColumn(Object o, boolean isIncrement) {
        StringBuilder sb = new StringBuilder();
        if (isIncrement) sb.append(o.toString());
        else sb.append(TEXT.javaClass().isInstance(o) ? "'" + o.toString() + "'" : o.toString());
        this.updateValue = sb.toString();
    }

    @Override
    public String toString() {
        return updateValue;
    }

}
