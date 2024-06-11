package com.cryp73r.utils;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.sql.Date;
import java.text.SimpleDateFormat;

import com.cryp73r.model.ViewInfo;

public class SQLServerSchemaCloner {

    public void extractSchemaAndDataToFBackup(String sourceConnectionString, String filePath) {
        final Set<String> addedTableSet = new HashSet<>();
        final Set<String> uniqueConstraintSet = new HashSet<>();
        final Map<String, String> columnDefaultValueMap = new HashMap<>();

        try (Connection connection = DriverManager.getConnection(sourceConnectionString); BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {

            DatabaseMetaData metaData = connection.getMetaData();

            // Create Table
            ResultSet tables = metaData.getTables(null, "dbo", "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                createTableWithPrimaryKey(connection, writer, tableName, columnDefaultValueMap, uniqueConstraintSet);
            }
            System.out.println("\r[V] CREATE Queries written successfully");
            tables.close();

            // Create Foreign, Unique Keys and Indexes
            tables = metaData.getTables(null, "dbo", "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");

                // Create foreign & unique key constraints
                writeForeignAndUniqueKeys(connection, writer, tableName, uniqueConstraintSet);

                // Create indexes on the table
                writeIndexes(connection, writer, tableName, uniqueConstraintSet);
            }
            System.out.println("\r[V] Foreign & Unique Keys and Indexes written successfully");
            tables.close();

            // Insert Data into Tables
            tables = metaData.getTables(null, "dbo", "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                insertDataInHierarchy(connection, writer, tableName, addedTableSet);
            }
            System.out.println("\r[V] INSERT Queries written successfully");
            tables.close();

            // Create views on tables and views
            createView(connection, writer);
            System.out.println("\r[V] CREATE Queries written successfully for Views");

            // Create triggers
            createTrigger(connection, writer);
            System.out.println("\r[V] Triggers written successfully");

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    public void loadSchemaAndDataFromBackup(String filePath, String destinationConnectionString) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath)); Connection connection = DriverManager.getConnection(destinationConnectionString); Statement statement = connection.createStatement()) {
            StringBuilder queryBuilder = new StringBuilder();
            String line;
            long i = 1;
            while ((line = reader.readLine()) != null) {
                System.out.print("\rCurrently Executing Line No.: " + i++);
                if (line.trim().startsWith("/** END **/")) {
                    statement.execute(queryBuilder.toString());
                    queryBuilder.setLength(0);
                    continue;
                }
                if (line.trim().startsWith("/** START **/")) continue;
                queryBuilder.append(line).append("\n");
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }

    private void createTableWithPrimaryKey(Connection connection, BufferedWriter writer, String tableName, Map<String, String> columnDefaultValueMap, Set<String> uniqueConstraintSet) throws SQLException, IOException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columns = metaData.getColumns(null, null, tableName, null);
        writer.write("/** START **/\n");
        writer.write("CREATE TABLE " + tableName + " (");
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            String columnType = columns.getString("TYPE_NAME");
            String constraintDefinition = columns.getString("COLUMN_DEF");
            int columnSize = columns.getInt("COLUMN_SIZE");
            int decimalDigits = columns.getInt("DECIMAL_DIGITS");
            int nullable = columns.getInt("NULLABLE");
            String isNullable = (nullable == DatabaseMetaData.columnNullable) ? "NULL" : "NOT NULL";

            String columnDefinition = columnName + " " + columnType;
            if (columnType.equalsIgnoreCase("DECIMAL") || columnType.equalsIgnoreCase("NUMERIC")) {
                columnDefinition += "(" + columnSize + ", " + decimalDigits + ")";
            } else if (columnType.equalsIgnoreCase("CHAR") || columnType.equalsIgnoreCase("VARCHAR") || columnType.equalsIgnoreCase("NVARCHAR") || columnType.equalsIgnoreCase("VARBINARY") || columnType.equalsIgnoreCase("NCHAR")) {
                if (columnSize <= 8000) columnDefinition += "(" + columnSize + ")";
                else columnDefinition += "(max)";
            }
            columnDefinition += " " + isNullable + ", ";


            writer.write(columnDefinition);
            if (constraintDefinition != null) {
                columnDefaultValueMap.put(columnName, constraintDefinition);
            }
        }

        // Add Primary Key
        ResultSet primaryKey = metaData.getPrimaryKeys(null, null, tableName);
        StringBuilder compositeColumns = new StringBuilder();
        String pkName = "";
        while (primaryKey.next()) {
            pkName = (primaryKey.getString("PK_NAME") != null)?primaryKey.getString("PK_NAME"):pkName;
            compositeColumns.append(primaryKey.getString("COLUMN_NAME")).append(",");
        };
        if (compositeColumns.length() > 0) {
            writer.write("CONSTRAINT " + pkName + " PRIMARY KEY (" + compositeColumns.substring(0, compositeColumns.length()-1) + ")");
            uniqueConstraintSet.add(pkName);
        }
        writer.write(");\n");
        writer.write("/** END **/\n");

        // Write Default value constraints
        for (Map.Entry<String, String> entry : columnDefaultValueMap.entrySet()) {
            writeDefaultConstraint(connection, writer, tableName, entry.getKey(), entry.getValue());
        }
        columnDefaultValueMap.clear();
    }

    private void writeDefaultConstraint(Connection connection, BufferedWriter writer, String tableName, String columnName, String constraintDefinition) throws SQLException, IOException {
        String query = "select o.name from sysobjects o inner join syscolumns c on o.id = c.cdefault inner join sysobjects t on c.id = t.id where o.xtype = 'D' and c.name = ? and t.name = ?";
        PreparedStatement pstmt = connection.prepareStatement(query);
        pstmt.setString(1, columnName);
        pstmt.setString(2, tableName);

        ResultSet rsdc = pstmt.executeQuery();
        while (rsdc.next()) {
            String constraintName = rsdc.getString("name");
            writer.write("/** START **/\n");
            writer.write("ALTER TABLE " + tableName + " ADD CONSTRAINT " + constraintName + " DEFAULT " + constraintDefinition + " FOR " + columnName +";\n");
            writer.write("/** END **/\n");
        }
    }

    private void writeForeignAndUniqueKeys(Connection connection, BufferedWriter writer, String tableName, Set<String> uniqueConstraintSet) throws SQLException, IOException {
        DatabaseMetaData metaData = connection.getMetaData();

        // Write Foreign Key constraints
        ResultSet fks = metaData.getImportedKeys(null, null, tableName);
        while (fks.next()) {
            String fkTableName = fks.getString("FKTABLE_NAME");
            String fkColumnName = fks.getString("FKCOLUMN_NAME");
            String fkName = fks.getString("FK_NAME");
            String pkTableName = fks.getString("PKTABLE_NAME");
            String pkColumnName = fks.getString("PKCOLUMN_NAME");
            String deleteRule = "NO ACTION";
            String updateRule = "NO ACTION";
            String query = "SELECT CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS WHERE CONSTRAINT_NAME = ? ;";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, fkName);
            ResultSet fkDeleteUpdateRule = pstmt.executeQuery();
            while (fkDeleteUpdateRule.next()) {
                deleteRule = fkDeleteUpdateRule.getString("DELETE_RULE");
                updateRule = fkDeleteUpdateRule.getString("UPDATE_RULE");
            }

            writer.write("/** START **/\n");
            writer.write("ALTER TABLE " + fkTableName + " ADD CONSTRAINT " + fkName +" FOREIGN KEY (" + fkColumnName + ") REFERENCES " + pkTableName + "(" + pkColumnName  + ") ON UPDATE "+ updateRule + " ON DELETE " + deleteRule + ";\n");
            writer.write("/** END **/\n");
        }

        // Write Unique Key constraints
        String uniqueConstraintsQuery = "SELECT CONSTRAINT_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE TABLE_NAME = ? AND CONSTRAINT_NAME IN ( SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'UNIQUE')";
        PreparedStatement ps = connection.prepareStatement(uniqueConstraintsQuery);
        ps.setString(1, tableName);
        ResultSet uks = ps.executeQuery();
        Map<String, String> uniqueColumnsMap = new HashMap<>();
        while (uks.next()) {
            String constraintName = uks.getString("CONSTRAINT_NAME");
            String columnName = uks.getString("COLUMN_NAME");
            if (constraintName != null && uniqueColumnsMap.containsKey(constraintName)) {
                String existingColumns = uniqueColumnsMap.get(constraintName);
                existingColumns += ", " + columnName;
                uniqueColumnsMap.put(constraintName, existingColumns);
            } else {
                uniqueColumnsMap.put(constraintName, columnName);
            }
        }
        uks.close();
        uks = ps.executeQuery();
        while (uks.next()) {
            String constraintName = uks.getString("CONSTRAINT_NAME");
            if (constraintName != null && !(uniqueConstraintSet.contains(constraintName))) {
                String columnNames = uniqueColumnsMap.get(constraintName);
                writer.write("/** START **/\n");
                writer.write("ALTER TABLE " + tableName + " ADD CONSTRAINT " + constraintName + " UNIQUE (" + columnNames + ");\n");
                writer.write("/** END **/\n");
                uniqueConstraintSet.add(constraintName);
            }
        }
    }

    private void writeIndexes(Connection connection, BufferedWriter writer, String tableName, Set<String> uniqueConstraintSet) throws SQLException, IOException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet indexes = metaData.getIndexInfo(null, null, tableName, false, false);
        Map<String, String> indexColumnsMap = new HashMap<>();
        while (indexes.next()) {
            String indexName = indexes.getString("INDEX_NAME");
            if (indexName != null && (!(uniqueConstraintSet.contains(indexName)))) {
                String columnName = indexes.getString("COLUMN_NAME");
                if (indexColumnsMap.containsKey(indexName)) {
                    String existingColumns = indexColumnsMap.get(indexName);
                    existingColumns += ", " + columnName;
                    indexColumnsMap.put(indexName, existingColumns);
                } else {
                    indexColumnsMap.put(indexName, columnName);
                }
            }
        }
        indexes.close();
        indexes = metaData.getIndexInfo(null, null, tableName, false, false);
        Set<String> indexNamesAdded = new HashSet<>();
        while (indexes.next()) {
            String indexName = indexes.getString("INDEX_NAME");
            if ((indexName != null &&  !(uniqueConstraintSet.contains(indexName))) && !indexNamesAdded.contains(indexName)) {
                boolean nonUnique = indexes.getBoolean("NON_UNIQUE");
                String columnNames = indexColumnsMap.get(indexName);
                writer.write("/** START **/\n");
                writer.write("CREATE " + (nonUnique ? "INDEX " : "UNIQUE INDEX ") + indexName + " ON " + tableName + " (" + columnNames + ");\n");
                writer.write("/** END **/\n");
                indexNamesAdded.add(indexName);
            }
        }
    }

    private void insertDataInHierarchy(Connection connection, BufferedWriter writer, String tableName, Set<String> addedTableSet) throws SQLException, IOException {
        if (addedTableSet.contains(tableName)) return;

        String query = "SELECT object_name(referenced_object_id) AS 'PARENT_TABLE' FROM sys.foreign_keys WHERE object_name(parent_object_id) = ? ;";
        PreparedStatement pstmt = connection.prepareStatement(query);
        pstmt.setString(1, tableName);
        ResultSet parentSet = pstmt.executeQuery();
        while (parentSet.next()) {
            String pkTableName = parentSet.getString("PARENT_TABLE");
            if (!(tableName.equals(pkTableName))) insertDataInHierarchy(connection, writer, pkTableName, addedTableSet);
        }
        writeInsertQuery(connection, writer, tableName);
        addedTableSet.add(tableName);
    }

    private void writeInsertQuery(Connection connection, BufferedWriter writer, String tableName) throws SQLException, IOException {
        String query = "SELECT * FROM " + tableName + ";";
        ResultSet data = connection.createStatement().executeQuery(query);
        ResultSetMetaData rsMetaData = data.getMetaData();
        int columnCount = rsMetaData.getColumnCount();
        while (data.next()) {
            writer.write("/** START **/\n");
            writer.write("INSERT INTO " + tableName + " VALUES (");

            for (int i = 1; i <= columnCount; i++) {
                int columnType = rsMetaData.getColumnType(i);

                switch (columnType) {
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.SMALLINT:
                    case Types.TINYINT:
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        writer.write((data.getString(i) == null) ? "NULL" : data.getString(i));
                        break;
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        writer.write(String.valueOf(data.getDouble(i)));
                        break;
                    case Types.BOOLEAN:
                        writer.write(String.valueOf(data.getBoolean(i)));
                        break;
                    case Types.DATE:
                        Date date = data.getDate(i);
                        if (date == null) writer.write("NULL");
                        else {
                            String formattedDate = new SimpleDateFormat("yyyy-MM-dd").format(date);
                            writer.write("CONVERT(DATETIME, '" + formattedDate + "')");
                        }
                        break;
                    case Types.TIME:
                        Time time = data.getTime(i);
                        if (time == null) writer.write("NULL");
                        else {
                            String formattedTime = new SimpleDateFormat("HH:mm:ss").format(time);
                            writer.write("CONVERT(DATETIME, '" + formattedTime + "')");
                        }
                        break;
                    case Types.TIMESTAMP:
                        Timestamp timestamp = data.getTimestamp(i);
                        if (timestamp == null) writer.write("NULL");
                        else {
                            String formattedTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp);
                            writer.write("CONVERT(DATETIME, '" + formattedTimestamp + "')");
                        }
                        break;
                    case Types.BLOB:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        if (data.getBytes(i) != null) {
                            String hexString = toHexString(data.getBytes(i));
                            String columnData = String.format("0x%s", hexString);
                            String finalColumnData = "CAST(" + columnData + " AS VARBINARY(MAX))";
                            writer.write(finalColumnData);
                        } else writer.write("NULL");
                        break;
                    default:
                        String value = data.getString(i);
                        if (value != null) {
                            writer.write("'" + value.replace("'", "''").trim() + "'");
                        } else {
                            writer.write("NULL");
                        }
                        break;
                }

                if (i < columnCount) {
                    writer.write(", ");
                }
            }

            writer.write(");\n");
            writer.write("/** END **/\n");
        }
    }

    private String toHexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }

    private void createView(Connection connection, BufferedWriter writer) throws SQLException, IOException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet views = metaData.getTables(null, "dbo", "%", new String[]{"VIEW"});
        List<ViewInfo> viewList = new ArrayList<>();
        while (views.next()) {
            String viewName = views.getString("TABLE_NAME");
            String sql = "SELECT definition, create_date FROM sys.objects o JOIN sys.sql_modules m ON m.object_id = o.object_id WHERE o.object_id = object_id('dbo." + viewName + "') AND o.type = 'V'";
            try (PreparedStatement statement = connection.prepareStatement(sql); ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String viewDefinition = rs.getString("definition");
                    Timestamp createDate = rs.getTimestamp("create_date");
                    viewList.add(new ViewInfo(viewName, viewDefinition, createDate));
                }
            }
        }
        views.close();

        viewList.sort((v1, v2) -> v1.getCreateDate().compareTo(v2.getCreateDate()));

        for (ViewInfo viewInfo : viewList) {
            writer.write("/** START **/\n");
            writer.write(viewInfo.getViewDefinition() + "\n");
            writer.write("/** END **/\n");
        }
    }

    private void createTrigger(Connection connection, BufferedWriter writer) throws SQLException, IOException {
        String query = "SELECT name, OBJECT_DEFINITION(OBJECT_ID) AS definition FROM sys.triggers";
        ResultSet rs = connection.createStatement().executeQuery(query);
        while (rs.next()) {
            String triggerDefinition = rs.getString("definition");
            writer.write("/** START **/\n");
            writer.write(triggerDefinition + "\n");
            writer.write("/** END **/\n");
        }
    }
}
