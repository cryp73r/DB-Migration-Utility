package com.cryp73r;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.cryp73r.schemaInterface.SchemaCloner;
import com.cryp73r.utils.OracleDBSchemaCloner;
import com.cryp73r.utils.SQLServerSchemaCloner;


public class DBDumpUtility {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("Action Type [import: only import from file, export: only export into file, both: perform export and then import in target db]: ");
        String actionType = br.readLine();
        String databaseType = null;
        String sourceURL = null;
        String targetURL = null;
        String filePath = null;
        String username = null;
        if (actionType.equals("import")) {
            System.out.print("Database Type [oracle, mssql]: ");
            databaseType = br.readLine().toLowerCase();
            SchemaCloner schemaCloner;
            if (databaseType.equals("oracle")) {
                schemaCloner = new OracleDBSchemaCloner();
            } else {
                schemaCloner = new SQLServerSchemaCloner();
            }
            System.out.print("Target Database Connection String [jdbc:oracle:thin:<username>/<password>@//<ip>:<port>/<ServiceName>, jdbc:sqlserver://<ip>:<port>;DatabaseName=<Schema Name>;encrypt=true;trustServerCertificate=true;user=<username>;password=<password>;]: ");
            targetURL = br.readLine();
            System.out.print("SQL File Path [D:\\dbDump.sql or /home/infamdm/dbDump.sql]: ");
            filePath = br.readLine();
            System.out.print("Username of Database user: ");
            username = br.readLine();

            System.out.println("*** STARTING DATA IMPORT ***");
            System.out.println("*** DATA IMPORT STARTED ***");
            schemaCloner.loadSchemaAndDataFromBackup(filePath, targetURL);
            System.out.println("\n*** DATA IMPORT ENDED ***");
            System.out.println("Please check for the backup file generated at " + filePath);
        } else if (actionType.equals("export")) {
            System.out.print("Database Type [oracle, mssql]: ");
            databaseType = br.readLine().toLowerCase();
            SchemaCloner schemaCloner;
            if (databaseType.equals("oracle")) {
                schemaCloner = new OracleDBSchemaCloner();
            } else {
                schemaCloner = new SQLServerSchemaCloner();
            }
            System.out.print("Source Database Connection String [jdbc:oracle:thin:<username>/<password>@//<ip>:<port>/<ServiceName>, jdbc:sqlserver://<ip>:<port>;DatabaseName=<Schema Name>;encrypt=true;trustServerCertificate=true;user=<username>;password=<password>;]: ");
            sourceURL = br.readLine();
            System.out.print("SQL File Path [D:\\dbDump.sql or /home/infamdm/dbDump.sql]: ");
            filePath = br.readLine();
            System.out.print("Username of Database user: ");
            username = br.readLine();

            System.out.println("*** STARTING DATA EXPORT ***");
            System.out.println("*** DATA EXPORT STARTED ***");
            schemaCloner.extractSchemaAndDataToFBackup(sourceURL, filePath, username);
            System.out.println("*** DATA EXPORT ENDED ***");
        } else if (actionType.equals("both")) {
            System.out.print("Database Type [oracle, mssql]: ");
            databaseType = br.readLine().toLowerCase();
            SchemaCloner schemaCloner;
            if (databaseType.equals("oracle")) {
                schemaCloner = new OracleDBSchemaCloner();
            } else {
                schemaCloner = new SQLServerSchemaCloner();
            }
            System.out.print("Source Database Connection String [jdbc:oracle:thin:<username>/<password>@//<ip>:<port>/<ServiceName>, jdbc:sqlserver://<ip>:<port>;DatabaseName=<Schema Name>;encrypt=true;trustServerCertificate=true;user=<username>;password=<password>;]: ");
            sourceURL = br.readLine();
            System.out.print("Target Database Connection String [jdbc:oracle:thin:<username>/<password>@//<ip>:<port>/<ServiceName>, jdbc:sqlserver://<ip>:<port>;DatabaseName=<Schema Name>;encrypt=true;trustServerCertificate=true;user=<username>;password=<password>;]: ");
            targetURL = br.readLine();
            System.out.print("SQL File Path [D:\\dbDump.sql or /home/infamdm/dbDump.sql]: ");
            filePath = br.readLine();
            System.out.print("Username of Database user: ");
            username = br.readLine();

            System.out.println("*** STARTING DATA EXPORT ***");
            System.out.println("*** DATA EXPORT STARTED ***");
            schemaCloner.extractSchemaAndDataToFBackup(sourceURL, filePath, username);
            System.out.println("*** DATA EXPORT ENDED ***");

            System.out.println("*** STARTING DATA IMPORT ***");
            System.out.println("*** DATA IMPORT STARTED ***");
            schemaCloner.loadSchemaAndDataFromBackup(filePath, targetURL);
            System.out.println("\n*** DATA IMPORT ENDED ***");
            System.out.println("Please check for the backup file generated at " + filePath);
        } else {
            System.out.println("[X] Invalid Action Type chosen. Please choose between import/export/both");
        }
    }

}