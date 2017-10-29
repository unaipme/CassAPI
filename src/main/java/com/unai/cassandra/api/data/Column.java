package com.unai.cassandra.api.data;

import com.unai.cassandra.api.DataType;

public class Column {

    private String name;
    private DataType type;
    private boolean isPartitionKey;
    private boolean isClusteringKey;

    public static Builder builder() {
        return new Builder();
    }

    public String getName() {
        return name;
    }

    public DataType getType() {
        return type;
    }

    public boolean isPartitionKey() {
        return isPartitionKey;
    }

    public boolean isClusteringKey() {
        return isClusteringKey;
    }

    private void setName(String name) {
        this.name = name;
    }

    private void setType(DataType type) {
        this.type = type;
    }

    private void setPartitionKey(boolean partitionKey) {
        isPartitionKey = partitionKey;
    }

    private void setClusteringKey(boolean clusteringKey) {
        isClusteringKey = clusteringKey;
    }

    public static class Builder {

        private Column column;

        public Builder() {
            this.column = new Column();
        }

        public Builder withName(String name) {
            column.setName(name);
            return this;
        }

        public Builder withType(DataType type) {
            column.setType(type);
            return this;
        }

        public Builder isPartitionKey(boolean b) {
            column.setPartitionKey(b);
            return this;
        }

        public Builder isPartitionKey() {
            return isPartitionKey(true);
        }

        public Builder isClusteringKey(boolean b) {
            column.setClusteringKey(b);
            return this;
        }

        public Builder isClusteringKey() {
            return isClusteringKey(true);
        }

        public Column build() {
            return column;
        }

    }

}
