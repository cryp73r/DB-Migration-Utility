# DB Migration Utility
#### The primary use of this utility is to create SQL queries based on the tables, views, data, various kinds of keys, indexes and triggers.

__This utility supports below databases:__
- Microsoft SQL Server (all versions, supporting JDBC Driver v9.4.1.jre8)
- Oracle Database (all versions, supporting OJDBC 8)

## How to use?
1. Clone the repository in your local and open as maven Project in the IDE of your choice
2. Run `mvn clean install` from root directory to download required JDBC drivers.
3. Export/Build the project as JAR, let's say `DBDumpUtility.jar`.
4. The JAR file will prompt for the arguments it will need.


## Future Scope
1. Introduce multi-threading to improve the performance.
2. Crete a Web Project to provide a UI for migration instead of directly using JAR.