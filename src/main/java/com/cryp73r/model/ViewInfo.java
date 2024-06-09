package com.cryp73r.model;

public class ViewInfo {
    private final String viewName;
    private final String viewDefinition;
    private final java.sql.Timestamp createDate;

    public ViewInfo(String viewName, String viewDefinition, java.sql.Timestamp createDate) {
        this.viewName = viewName;
        this.viewDefinition = viewDefinition;
        this.createDate = createDate;
    }

    public String getViewName() {
        return viewName;
    }

    public String getViewDefinition() {
        return viewDefinition;
    }

    public java.sql.Timestamp getCreateDate() {
        return createDate;
    }
}
