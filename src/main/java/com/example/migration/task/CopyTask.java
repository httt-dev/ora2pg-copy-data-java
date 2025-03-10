package com.example.migration.task;

import com.example.migration.config.DataSourceConfig;
import com.example.migration.util.MetadataUtil;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;
import java.sql.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CopyTask implements Runnable {
    private final DataSourceConfig sourceConfig;
    private final DataSourceConfig targetConfig;
    private final String table;
    private final List<String> columns;
    private final long start;
    private final long end;
    private final String query;
    private final HikariDataSource oraclePool;
    private final HikariDataSource postgresPool;

    public CopyTask(DataSourceConfig source, DataSourceConfig target, String table, List<String> columns, long start, long end, String query, HikariDataSource oraclePool, HikariDataSource postgresPool) {
        this.sourceConfig = source;
        this.targetConfig = target;
        this.table = table;
        this.columns = columns;
        this.start = start;
        this.end = end;
        this.query = query;
        this.oraclePool = oraclePool;
        this.postgresPool = postgresPool;
    }

    @Override
    public void run() {
        try (Connection oracleConn = oraclePool.getConnection()) {
            try (Connection pgConn = postgresPool.getConnection()) {
                try (PreparedStatement pstmt = oracleConn.prepareStatement(query)) {
                    pstmt.setLong(1, start);
                    pstmt.setLong(2, end);
                    pstmt.setFetchSize(100000); // Tối ưu fetch size
                    ResultSet rs = pstmt.executeQuery();

                    // Đo thời gian và logging
                    System.out.println("Task started: " + table + " [" + start + " → " + end + "]");
                    AtomicInteger processedRows = new AtomicInteger(0);
                    AtomicInteger errorCount = new AtomicInteger(0);

                    // Xây dựng query COPY cho PostgreSQL
                    String copySql = "COPY " + table + " (" + String.join(", ", columns) + ") FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL 'NULL')";
                    BaseConnection rawPgConn = (BaseConnection) pgConn.unwrap(BaseConnection.class);
                    CopyManager copyManager = new CopyManager(rawPgConn);

                    // Sử dụng PipedStream để truyền dữ liệu
                    PipedOutputStream pos = new PipedOutputStream();
                    PipedInputStream pis = new PipedInputStream(pos);

                    Thread writerThread = new Thread(() -> {
                        try (OutputStreamWriter writer = new OutputStreamWriter(pos)) {
                            while (rs.next()) {
                                processedRows.incrementAndGet();
                                try {
                                    writeRowToCsv(rs, columns, writer);
                                    writer.write("\n");
                                } catch (Exception e) {
                                    errorCount.incrementAndGet();
                                    System.err.println("Error writing row " + processedRows.get() + ": " + e.getMessage());
                                }
                            }
                            writer.flush();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        } finally {
                            try { pos.close(); } catch (IOException e) { e.printStackTrace(); }
                        }
                    });

                    writerThread.start();
                    copyManager.copyIn(copySql, pis);
                    writerThread.join();

                    // Logging kết quả
                    System.out.println("Task completed: " + table +
                                      " → Processed: " + processedRows.get() +
                                      ", Errors: " + errorCount.get());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Hàm viết row thành CSV
    private void writeRowToCsv(ResultSet rs, List<String> columns, Writer writer) throws SQLException, IOException {
        for (int i = 0; i < columns.size(); i++) {
            String columnName = columns.get(i);
            Object value = rs.getObject(columnName);

            if (value == null) {
                writer.write("NULL");
            } else {
                writer.write("\"" + value.toString().replace("\"", "\"\"") + "\"");
            }

            if (i < columns.size() - 1) {
                writer.write(",");
            }
        }
    }
}