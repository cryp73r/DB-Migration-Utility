# DB Migration Utility
#### The primary use of this utility is to create SQL queries based on the tables, views, data, various kinds of keys, indexes and triggers.

__Currently, this utility supports below databases:__
- Microsoft SQL Server (all versions, supporting JDBC Driver v9.4.1.jre8)

__Upcoming Database support:__
- Oracle Database

## How to use?
1. Clone the repository in your local and open as maven Project in the IDE of your choice
2. Run `mvn clean install` from root directory to download required JDBC drivers.
3. Export/Build the project as JAR, let's say `DBDumpUtility.jar`.
4. The JAR file will require 3 arguments to be passed to function
    - SourceConnectionString: JDBC connection URL to the source database
    - BackupFilePath: Complete path where the exported queries will be saved including the filename and extension of the file
    - TargetConnectionString: JDBC connection URL to the target database

    ### Example (for SQL Server)
    `java -jar DBDumpUtility.jar jdbc:sqlserver://localhost:1433;DatabaseName=mdm_sample;encrypt=true;trustServerCertificate=true;user=mdm_sample;password=!!cmx!!; D:\backup.sql jdbc:sqlserver://localhost:1433;DatabaseName=ors_dump;encrypt=true;trustServerCertificate=true;user=ors_dump;password=!!cmx!!;`