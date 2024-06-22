package com.cryp73r.model;

public class PrimaryKeySequence {
    private final int keySeq;
    private final String columnName;

    public PrimaryKeySequence(int keySeq, String columnName) {
        this.keySeq = keySeq;
        this.columnName = columnName;
    }

    public int getKeySeq() {
        return keySeq;
    }

    public String getColumnName() {
        return columnName;
    }
}
