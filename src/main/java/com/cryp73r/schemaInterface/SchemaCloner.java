package com.cryp73r.schemaInterface;

public interface SchemaCloner {

    public void extractSchemaAndDataToFBackup(String sourceConnectionString, String filePath, String username);

    public void loadSchemaAndDataFromBackup(String filePath, String destinationConnectionString);

}
