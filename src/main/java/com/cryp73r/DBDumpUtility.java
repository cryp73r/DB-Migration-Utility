package com.cryp73r;

import com.cryp73r.utils.SQLServerSchemaCloner;


public class DBDumpUtility {
    public static void main(String[] args) {
        System.out.println("*** STARTING DATA EXPORT ***");
        String sourceConnectionString = args[0];
        String backupFilePath = args[1];
        SQLServerSchemaCloner sqlServerSchemaCloner = new SQLServerSchemaCloner();

        System.out.println("*** DATA EXPORT STARTED ***");
        sqlServerSchemaCloner.extractSchemaAndDataToFBackup(sourceConnectionString, backupFilePath);
        System.out.println("*** DATA EXPORT ENDED ***");

        System.out.println("*** STARTING DATA IMPORT ***");
        System.out.println("*** DATA IMPORT STARTED ***");
        String targetConnectionString = args[2];

        sqlServerSchemaCloner.loadSchemaAndDataFromBackup(backupFilePath, targetConnectionString);
        System.out.println("\n*** DATA IMPORT ENDED ***");
        System.out.println("Please check for the backup file generated at " + backupFilePath);
    }

}