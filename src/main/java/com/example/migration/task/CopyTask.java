package com.example.migration.task;

import com.example.migration.config.DataSourceConfig;
import com.example.migration.util.MetadataUtil;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.List;

public class CopyTask implements Runnable {
    private final DataSourceConfig sourceConfig;
    private final DataSourceConfig targetConfig;
    private final String table;
    private final List<MetadataUtil.Column> columns;
    private final long start;
    private final long end;
    private final String query;

    public CopyTask(DataSourceConfig source, DataSourceConfig target, String table,
                    List<MetadataUtil.Column> columns, long start, long end, String query) {
        this.sourceConfig = source;
        this.targetConfig = target;
        this.table = table;
        this.columns = columns;
        this.start = start;
        this.end = end;
        this.query = query;
    }

    @Override
    public void run() {
        try (Connection oracleConn = DriverManager.getConnection(sourceConfig.getUrl(), sourceConfig.getUsername(), sourceConfig.getPassword());
             Connection pgConn = DriverManager.getConnection(targetConfig.getUrl(), targetConfig.getUsername(), targetConfig.getPassword())) {

            try (PreparedStatement pstmt = oracleConn.prepareStatement(query)) {
                pstmt.setLong(1, start);
                pstmt.setLong(2, end);
                pstmt.setFetchSize(50000);
                ResultSet rs = pstmt.executeQuery();

                CopyManager copyManager = new CopyManager((BaseConnection) pgConn);
                String copySql = "COPY " + table + " (" +
                        columns.stream().map(MetadataUtil.Column::getName).collect(java.util.stream.Collectors.joining(",")) +
                        ") FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '')";

                PipedOutputStream pos = new PipedOutputStream();
                PipedInputStream pis = new PipedInputStream(pos);

                Thread writerThread = new Thread(() -> {
                    try (OutputStreamWriter writer = new OutputStreamWriter(pos, StandardCharsets.UTF_8)) {
                        while (rs.next()) {
                            writeRowToCsv(rs, columns, writer);
                            writer.write("\n");
                        }
                        writer.flush();
                    } catch (IOException | SQLException e) {
                        throw new RuntimeException(e);
                    } finally {
                        try { pos.close(); } catch (IOException e) { e.printStackTrace(); }
                    }
                });
                writerThread.start();

                copyManager.copyIn(copySql, pis);
                writerThread.join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeRowToCsv(ResultSet rs, List<MetadataUtil.Column> columns, Writer writer)
            throws SQLException, IOException {
        for (int i = 0; i < columns.size(); i++) {
            MetadataUtil.Column column = columns.get(i);
            Object value = null;

            if (isLobColumn(column.getType())) {
                value = handleLobColumn(rs, column);
            } else {
                value = rs.getObject(column.getName());
            }

            // Xử lý giá trị null và escape CSV
            if (value == null) {
                writer.write("");
            } else {
                writer.write("\"");
                writer.write(value.toString().replace("\"", "\"\""));
                writer.write("\"");
            }

            if (i < columns.size() - 1) {
                writer.write(",");
            }
        }
    }

    private boolean isLobColumn(String type) {
        return type.equalsIgnoreCase("BLOB") ||
                type.equalsIgnoreCase("CLOB") ||
                type.equalsIgnoreCase("NCLOB") ||
                type.equalsIgnoreCase("BFILE");
    }

    private Object handleLobColumn(ResultSet rs, MetadataUtil.Column column) throws SQLException, IOException {
        String typeName = column.getType();
        String columnName = column.getName();

        if (typeName.equalsIgnoreCase("BLOB")) {
            Blob blob = rs.getBlob(columnName);
            if (blob == null) return null;
            try (InputStream is = blob.getBinaryStream()) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                byte[] data = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(data, 0, data.length)) != -1) {
                    buffer.write(data, 0, bytesRead);
                }
                return java.util.Base64.getEncoder().encodeToString(buffer.toByteArray());
            }
        } else if (typeName.equalsIgnoreCase("CLOB") || typeName.equalsIgnoreCase("NCLOB")) {
            Clob clob = rs.getClob(columnName);
            if (clob == null) return null;
            try (Reader reader = clob.getCharacterStream()) {
                StringWriter stringWriter = new StringWriter();
                char[] buffer = new char[8192];
                int charsRead;
                while ((charsRead = reader.read(buffer)) != -1) {
                    stringWriter.write(buffer, 0, charsRead);
                }
                return stringWriter.toString();
            }
        }
        return rs.getObject(columnName);
    }
}