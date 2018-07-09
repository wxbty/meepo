package org.bytesoft.bytejta.image;

import java.util.List;

public class Image {

    private List<LineFileds> line;

    private String schemaName;

    private String tableName;

    private boolean isbefore;

    public boolean isIsbefore() {
        return isbefore;
    }

    public void setIsbefore(boolean isbefore) {
        this.isbefore = isbefore;
    }

    public List<LineFileds> getLine() {
        return line;
    }

    public void setLine(List<LineFileds> line) {
        this.line = line;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
