package com.cryp73r.utils;

import com.cryp73r.model.PrimaryKeySequence;
import com.cryp73r.model.ViewInfo;

import com.cryp73r.schemaInterface.SchemaCloner;

import java.io.*;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

public class OracleDBSchemaCloner implements SchemaCloner {

    private final Set<String> hashSet = new HashSet<>();
    final Set<String> uniqueConstraintSet = new HashSet<>();
    //    Map<String, String> columnMap = new HashMap<>();
    Map<String, String> tableIndexMap = new HashMap<>();

    @Override
    public void extractSchemaAndDataToFBackup(String sourceConnectionString, String filePath, String username) {
        try (Connection connection = DriverManager.getConnection(sourceConnectionString); BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {

            DatabaseMetaData metaData = connection.getMetaData();

            // Create Table
            ResultSet tables = metaData.getTables(null, username, "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                createTableWithPrimaryKey(connection, writer, tableName);
            }
            System.out.println("\r[V] CREATE Queries written successfully");
            tables.close();

            // Commit Transaction
            writer.write("/** START **/\n");
            writer.write("commit\n");
            writer.write("/** END **/\n");

            // Create Foreign, Unique Keys and Indexes
            tables = metaData.getTables(null, username, "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");

                // Create indexes on the table
                writeIndexes(connection, writer, tableName);

                // Create foreign & unique key constraints
                writeForeignAndUniqueKeys(connection, writer, tableName);
            }
            System.out.println("\r[V] Foreign & Unique Keys and Indexes written successfully");
            tables.close();

            // Commit Transaction
            writer.write("/** START **/\n");
            writer.write("commit\n");
            writer.write("/** END **/\n");

            // Insert Data into Tables
            tables = metaData.getTables(null, username, "%", new String[]{"TABLE"});
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");
                insertDataInHierarchy(connection, writer, tableName);
            }
            System.out.println("\r[V] INSERT Queries written successfully");
            tables.close();

            // Commit Transaction
            writer.write("/** START **/\n");
            writer.write("commit\n");
            writer.write("/** END **/\n");

            // Create views on tables and views
            createView(connection, writer);
            System.out.println("\r[V] CREATE Queries written successfully for Views");

            // Commit Transaction
            writer.write("/** START **/\n");
            writer.write("commit\n");
            writer.write("/** END **/\n");

            // Create triggers
            createTrigger(connection, writer);
            System.out.println("\r[V] Triggers written successfully");

            // Commit Transaction
            writer.write("/** START **/\n");
            writer.write("commit\n");
            writer.write("/** END **/\n");

        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    @Override
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

    private void createTableWithPrimaryKey(Connection connection, BufferedWriter writer, String tableName) throws SQLException, IOException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columns = metaData.getColumns(null, null, tableName, null);
        writer.write("/** START **/\n");
        writer.write("CREATE TABLE " + tableName + " (");
        String columnDefinition = "";
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            String columnType = columns.getString("TYPE_NAME");
//            String constraintDefinition = columns.getString("COLUMN_DEF");
            int columnSize = columns.getInt("COLUMN_SIZE");
            int decimalDigits = columns.getInt("DECIMAL_DIGITS");
            int nullable = columns.getInt("NULLABLE");
            String isNullable = (nullable == DatabaseMetaData.columnNullable) ? "NULL" : "NOT NULL";

            String query = "SELECT DATA_DEFAULT FROM ALL_TAB_COLUMNS WHERE TABLE_NAME=? AND COLUMN_NAME=?";
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, tableName);
            pstmt.setString(2, columnName);
            ResultSet ddrs = pstmt.executeQuery();
            String defaultValue = "";
            while (ddrs.next()) {
                defaultValue = ddrs.getString("DATA_DEFAULT");
            }
            ddrs.close();
            pstmt.close();
            columnDefinition += columnName + " " + columnType;
            if (columnType.equalsIgnoreCase("NUMBER")) {
                columnDefinition += "(" + columnSize + ", " + decimalDigits + ")";
            } else if (columnType.equalsIgnoreCase("CHAR") || columnType.equalsIgnoreCase("VARCHAR2") || columnType.equalsIgnoreCase("NCHAR") || columnType.equalsIgnoreCase("NVARCHAR2")) {
                columnDefinition += "(" + columnSize + ")";
            }

            if (defaultValue != null && !defaultValue.isEmpty()) {
                columnDefinition += " DEFAULT " + defaultValue;
            }
            columnDefinition += " " + isNullable + ", ";

//            if (constraintDefinition != null) {
//                columnMap.put(columnName, constraintDefinition);
//            }
        }
        columns.close();
        writer.write(columnDefinition.substring(0, columnDefinition.length() - 2));

        // Add Primary Key
        ResultSet primaryKey = metaData.getPrimaryKeys(null, null, tableName);
        String pkName = "";
        List<PrimaryKeySequence> primaryKeySequenceList = new ArrayList<>();
        while (primaryKey.next()) {
            pkName = (primaryKey.getString("PK_NAME") != null) ? primaryKey.getString("PK_NAME") : pkName;
            String columnName = primaryKey.getString("COLUMN_NAME");
            int keySeq = primaryKey.getInt("KEY_SEQ");
            primaryKeySequenceList.add(new PrimaryKeySequence(keySeq, columnName));
        }
        primaryKey.close();

        primaryKeySequenceList.sort((p1, p2) -> Integer.compare(p1.getKeySeq(), p2.getKeySeq()));

        String compositeColumns = "";
        for (PrimaryKeySequence primaryKeySequence : primaryKeySequenceList) {
            compositeColumns += primaryKeySequence.getColumnName() + ",";
        }

        if (!compositeColumns.isEmpty()) {
            writer.write(", CONSTRAINT " + pkName + " PRIMARY KEY (" + compositeColumns.substring(0, compositeColumns.length() - 1) + ")");
            uniqueConstraintSet.add(pkName);
        }

        String query = "SELECT LOGGING FROM ALL_TABLES WHERE TABLE_NAME=?";
        PreparedStatement pstmt = connection.prepareStatement(query);
        pstmt.setString(1, tableName);
        ResultSet lrs = pstmt.executeQuery();
        boolean isLogging = false;
        while (lrs.next()) {
            String logging = lrs.getString("LOGGING");
            if (logging != null && !(logging.isEmpty())) {
                isLogging = logging.equals("YES");
            }
        }
        lrs.close();
        pstmt.close();
        writer.write(") " + (isLogging?"LOGGING":"NOLOGGING") + "\n");
        writer.write("/** END **/\n");

        // Write Default value constraints
//        for (Map.Entry<String, String> entry : columnMap.entrySet()) {
//            writeDefaultConstraint(connection, writer, tableName, entry.getKey(), entry.getValue());
//        }
//        columnMap.clear();
    }

//    private void writeDefaultConstraint(Connection connection, BufferedWriter writer, String tableName, String columnName, String constraintDefinition) throws SQLException, IOException {
//        String query = "SELECT constraint_name FROM all_constraints WHERE table_name = ? AND constraint_type = 'C' AND search_condition_vc LIKE ?";
//        PreparedStatement pstmt = connection.prepareStatement(query);
//        pstmt.setString(1, tableName);
//        pstmt.setString(2, "%" + columnName + "%");
//
//        ResultSet rsdc = pstmt.executeQuery();
//        while (rsdc.next()) {
//            String constraintName = rsdc.getString("constraint_name");
//            writer.write("/** START **/\n");
//            writer.write("ALTER TABLE " + tableName + " MODIFY (CONSTRAINT " + constraintName + " " + columnName + " DEFAULT " + constraintDefinition + ")\n");
//            writer.write("/** END **/\n");
//        }
//        rsdc.close();
//        pstmt.close();
//    }

    private void writeForeignAndUniqueKeys(Connection connection, BufferedWriter writer, String tableName) throws SQLException, IOException {
        DatabaseMetaData metaData = connection.getMetaData();

        // Write Foreign Key constraints
        ResultSet fks = metaData.getImportedKeys(null, null, tableName);
        while (fks.next()) {
            String fkTableName = fks.getString("FKTABLE_NAME");
            String fkColumnName = fks.getString("FKCOLUMN_NAME");
            String fkName = fks.getString("FK_NAME");
            String pkTableName = fks.getString("PKTABLE_NAME");
            String pkColumnName = fks.getString("PKCOLUMN_NAME");
            String deleteRule = fks.getString("DELETE_RULE");

            writer.write("/** START **/\n");
            writer.write("ALTER TABLE " + fkTableName + " ADD CONSTRAINT " + fkName + " FOREIGN KEY (" + fkColumnName + ") REFERENCES " + pkTableName + "(" + pkColumnName + ")\n");
            if (deleteRule != null && !(deleteRule.equals("1"))) {
                writer.write("ON DELETE CASCADE\n");
            }
            writer.write("/** END **/\n");
        }
        fks.close();

        // Write Unique Key constraints
        String uniqueConstraintsQuery = "SELECT constraint_name, column_name FROM all_cons_columns WHERE table_name = ? AND constraint_name IN (SELECT constraint_name FROM all_constraints WHERE constraint_type = 'U')";
        PreparedStatement ps = connection.prepareStatement(uniqueConstraintsQuery);
        ps.setString(1, tableName);
        ResultSet uks = ps.executeQuery();
        Map<String, String> uniqueColumnsMap = new HashMap<>();
        while (uks.next()) {
            String constraintName = uks.getString("constraint_name");
            String columnName = uks.getString("column_name");
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
            String constraintName = uks.getString("constraint_name");
            if (constraintName != null && !uniqueConstraintSet.contains(constraintName)) {
                String columnNames = uniqueColumnsMap.get(constraintName);
                writer.write("/** START **/\n");
                writer.write("ALTER TABLE " + tableName + " ADD CONSTRAINT " + constraintName + " UNIQUE (" + columnNames + ")\n");
                if (tableIndexMap.containsKey(tableName+columnNames)) {
                    writer.write("USING INDEX " + tableIndexMap.get(tableName+columnNames) + " ENABLE\n");
                }
                writer.write("/** END **/\n");
                uniqueConstraintSet.add(constraintName);
            }
        }
        uks.close();
        ps.close();
    }

    private void writeIndexes(Connection connection, BufferedWriter writer, String tableName) throws SQLException, IOException {
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
        Map<String, Integer> indexNamesAdded = new HashMap<>();
        while (indexes.next()) {
            String indexName = indexes.getString("INDEX_NAME");
            if ((indexName != null &&  !(uniqueConstraintSet.contains(indexName))) && !indexNamesAdded.containsKey(indexName)) {
                int unique = indexes.getInt("NON_UNIQUE");
                String columnNames = indexColumnsMap.get(indexName);
                writer.write("/** START **/\n");
                writer.write("CREATE " + ((unique==0)?"UNIQUE ":"") + "INDEX " + indexName + " ON " + tableName + " (" + columnNames + ") TABLESPACE \"CMX_INDX\"\n");
                writer.write("/** END **/\n");
                indexNamesAdded.put(indexName, 0);
                tableIndexMap.put(tableName+columnNames, indexName);
            }
        }
        indexes.close();
    }

    private void insertDataInHierarchy(Connection connection, BufferedWriter writer, String tableName) throws SQLException, IOException {
        if (hashSet.contains(tableName)) return;

        String query = "SELECT r.table_name AS PARENT_TABLE FROM user_constraints t JOIN user_constraints r ON t.r_constraint_name = r.constraint_name WHERE t.constraint_type = 'R' AND t.table_name = ?";
        PreparedStatement pstmt = connection.prepareStatement(query);
        pstmt.setString(1, tableName);
        ResultSet parentSet = pstmt.executeQuery();
        while (parentSet.next()) {
            String pkTableName = parentSet.getString("PARENT_TABLE");
            if (!(tableName.equals(pkTableName))) insertDataInHierarchy(connection, writer, pkTableName);
        }
        parentSet.close();
        pstmt.close();
        writeInsertQuery(connection, writer, tableName);
        hashSet.add(tableName);
    }

    private void writeInsertQuery(Connection connection, BufferedWriter writer, String tableName) throws SQLException, IOException {
        String query = "SELECT * FROM " + tableName;
        ResultSet data = connection.createStatement().executeQuery(query);
        ResultSetMetaData rsMetaData = data.getMetaData();
        int columnCount = rsMetaData.getColumnCount();
        while (data.next()) {
            StringBuilder sb = new StringBuilder();
            StringBuilder fplsb = new StringBuilder();
            StringBuilder lplsb = new StringBuilder();
            boolean isBlobPresent = false;
            boolean isClobPresent = false;
            fplsb.append("/** START **/\n");
            sb.append("INSERT INTO ").append(tableName).append(" VALUES (");

            for (int i = 1; i <= columnCount; i++) {
                int columnType = rsMetaData.getColumnType(i);

                switch (columnType) {
                    case Types.INTEGER:
                    case Types.BIGINT:
                    case Types.SMALLINT:
                    case Types.TINYINT:
                    case Types.NUMERIC:
                    case Types.DECIMAL:
                        sb.append((data.getString(i) == null) ? "NULL" : data.getString(i));
                        break;
                    case Types.FLOAT:
                    case Types.REAL:
                    case Types.DOUBLE:
                        sb.append(String.valueOf(data.getDouble(i)));
                        break;
                    case Types.BOOLEAN:
                        sb.append(String.valueOf(data.getBoolean(i)));
                        break;
                    case Types.DATE:
                        Date date = data.getDate(i);
                        if (date == null) sb.append("NULL");
                        else {
                            String formattedDate = new SimpleDateFormat("yyyy-MM-dd").format(date);
                            sb.append("TIMESTAMP '" + formattedDate + "'");
                        }
                        break;
                    case Types.TIME:
                        Time time = data.getTime(i);
                        if (time == null) sb.append("NULL");
                        else {
                            String formattedTime = new SimpleDateFormat("HH:mm:ss").format(time);
                            sb.append("TIMESTAMP '" + formattedTime + "'");
                        }
                        break;
                    case Types.TIMESTAMP:
                        Timestamp timestamp = data.getTimestamp(i);
                        if (timestamp == null) sb.append("NULL");
                        else {
                            String formattedTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(timestamp);
                            sb.append("TIMESTAMP '" + formattedTimestamp + "'");
                        }
                        break;
                    case Types.BLOB:
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        if (data.getBlob(i) != null) {
                            isBlobPresent = true;
                            Blob blob = data.getBlob(i);
                            fplsb.append("DECLARE\n")
                                    .append("  v_blob BLOB;\n")
                                    .append("  v_raw RAW(2000);\n")  // Smaller chunk size for RAW
                                    .append("BEGIN\n")
                                    .append("  DBMS_LOB.CREATETEMPORARY(v_blob, TRUE);\n");
                            try (InputStream is = blob.getBinaryStream()) {
                                byte[] buffer = new byte[2000]; // Chunk size of 2000 bytes
                                int bytesRead;
                                while ((bytesRead = is.read(buffer)) != -1) {
                                    String hexString = toHexString(buffer, bytesRead);
                                    fplsb.append("  v_raw := '").append(hexString).append("';\n")
                                            .append("  DBMS_LOB.WRITEAPPEND(v_blob, LENGTH(v_raw) / 2, v_raw);\n");
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                            sb.append("v_blob");
                            lplsb.append("  DBMS_LOB.FREETEMPORARY(v_blob);\n")
                                    .append("END;\n");
                        } else sb.append("NULL");
                        break;
                    default:
                        String value = data.getString(i);
                        if (value != null) {
                            if (value.length() > 2000) {
                                isClobPresent = true;
                                int CHUNK_SIZE = 2000;
                                int columnDataLength = value.length();
                                fplsb.append("DECLARE\n")
                                        .append("  v_clob CLOB;\n")
                                        .append("  v_varchar2 VARCHAR2(4000);\n")
                                        .append("BEGIN\n")
                                        .append("  DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);\n");
                                for (int j=0; j<columnDataLength; j+=CHUNK_SIZE) {
                                    int end = Math.min(j+CHUNK_SIZE, columnDataLength);
                                    String chunk = value.substring(j, end).replace("'", "''").trim();
                                    fplsb.append("  v_varchar2 := '").append(chunk).append("';\n")
                                            .append("  DBMS_LOB.WRITEAPPEND(v_clob, LENGTH(v_varchar2), v_varchar2);\n");
                                }
                                sb.append("v_clob");
                                lplsb.append("  DBMS_LOB.FREETEMPORARY(v_clob);\n")
                                        .append("END;\n");
                            } else sb.append("'" + value.replace("'", "''").trim() + "'");
                        } else {
                            sb.append("NULL");
                        }
                        break;
                }

                if (i < columnCount) {
                    sb.append(", ");
                }
            }
            if (isBlobPresent || isClobPresent) {
                sb.append(");\n");
                fplsb.append(sb.toString()).append(lplsb.toString()).append("/** END **/\n");
                writer.write(fplsb.toString());
            } else {
                sb.append(")\n").append("/** END **/\n");
                fplsb.append(sb.toString());
                writer.write(fplsb.toString());
            }
        }
        data.close();
    }

    private static String toHexString(byte[] bytes, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(String.format("%02X", bytes[i]));
        }
        return sb.toString();
    }

    private void createView(Connection connection, BufferedWriter writer) throws SQLException, IOException {
        List<ViewInfo> views = new ArrayList<>();
        String viewQuery = "SELECT view_name, text FROM user_views";
        Statement viewStmt = connection.createStatement();
        ResultSet viewRS = viewStmt.executeQuery(viewQuery);
        while (viewRS.next()) {
            String viewName = viewRS.getString("view_name");
            String viewText = viewRS.getString("text");
            views.add(new ViewInfo(viewName, viewText));
        }
        viewRS.close();
        viewStmt.close();
        for (ViewInfo view : views) {
            writer.write("/** START **/\n");
            writer.write("CREATE OR REPLACE VIEW " + view.getViewName() + " AS " + view.getViewDefinition() + "\n");
            writer.write("/** END **/\n");
        }
    }

    private void createTrigger(Connection connection, BufferedWriter writer) throws SQLException, IOException {
        List<String> triggers = new ArrayList<>();
        String triggerQuery = "SELECT description, trigger_body, crossedition FROM user_triggers";
        Statement triggerStmt = connection.createStatement();
        ResultSet triggerRS = triggerStmt.executeQuery(triggerQuery);
        while (triggerRS.next()) {
            String description = triggerRS.getString("description");
            String trigger_body = triggerRS.getString("trigger_body");
            boolean crossedition = triggerRS.getString("crossedition").equals("NO");
            String triggerDefinition = "CREATE OR REPLACE " + (crossedition?"NONEDITIONABLE TRIGGER ":"EDITIONABLE TRIGGER ") + description + "\n" + trigger_body;
            triggers.add(triggerDefinition);
        }
        triggerRS.close();
        triggerStmt.close();
        for (String trigger : triggers) {
            writer.write("/** START **/\n");
            writer.write(trigger + "\n");
            writer.write("/** END **/\n");
        }
    }
}
