package com.example.migration;

import com.example.migration.config.Config;
import com.example.migration.config.DataSourceConfig;
import com.example.migration.task.CopyTask;
import com.example.migration.util.MetadataUtil;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class DataMigration {
    private static HikariDataSource oraclePool;
    private static HikariDataSource postgresPool;

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Yaml yaml = new Yaml(new Constructor(Config.class));

        try (InputStream inputStream = DataMigration.class.getClassLoader().getResourceAsStream("config.yml")) {
            Config config = yaml.load(inputStream);

            // Khởi tạo pool connection cho Oracle và PostgreSQL
            oraclePool = createPool(config.getSource(), config.getWorkers() * 2);
            postgresPool = createPool(config.getTarget(), config.getWorkers() * 2);

            ExecutorService executor = Executors.newFixedThreadPool(config.getWorkers());

            for (String table : config.getTables()) {
                try (Connection oracleConn = oraclePool.getConnection()) {
                    List<String> columns = MetadataUtil.getColumns(oracleConn, table);
                    List<String> pkColumns = MetadataUtil.getPrimaryKeyColumns(oracleConn, table);
                    long rowCount = MetadataUtil.getRowCount(oracleConn, table);

                    if (rowCount == 0) {
                        System.out.println("Table " + table + " is empty. Skipping...");
                        continue;
                    }

                    // Tính chunkSize
                    long chunkSize = (rowCount + config.getWorkers() - 1) / config.getWorkers();
                    chunkSize = Math.max(chunkSize, 1);

                    List<MetadataUtil.Column> columnsWithTypes = MetadataUtil.getColumnsWithTypes(oracleConn, table);
                    String columnList = columnsWithTypes.stream()
                            .map(col -> col.getName().toLowerCase())
                            .collect(Collectors.joining(","));

//                    // Xây dựng query với ROW_NUMBER() dựa trên primary key
//                    String columnList = String.join(", ", columns);
//                    String orderByClause = String.join(", ", pkColumns);
//                    String query = String.format(
//                            "SELECT %s " +
//                            "FROM (" +
//                                "SELECT t.*, ROW_NUMBER() OVER (ORDER BY %s) AS rnum " +
//                                "FROM %s t" +
//                            ") " +
//                            "WHERE rnum BETWEEN ? AND ?",
//                            columnList,
//                            orderByClause,
//                            table
//                    );

                    String orderByClause;
                    if (!pkColumns.isEmpty()) {
                        orderByClause = String.join(", ", pkColumns); // Cột khóa ghép
                    } else {
                        orderByClause = "ROWID"; // Backup plan
                    }

                    // Xây dựng query với composite key
                    String query;
                    if (!pkColumns.isEmpty()) {
                        // Dùng composite key cho query BETWEEN
                        String pkCondition = pkColumns.stream()
                                .map(col -> col + " BETWEEN ? AND ?")
                                .collect(Collectors.joining(" AND "));
                        query = String.format(
                            "SELECT %s " +
                            "FROM %s " +
                            "WHERE (%s) " +
                            "ORDER BY %s",
                            columnList,
                            table,
                            pkCondition,
                            orderByClause
                        );
                    } else {
                        // Dùng ROWNUM cho bảng không có khóa
                        query = String.format(
                            "SELECT %s " +
                            "FROM (" +
                                "SELECT t.*, rownum AS rnum " +
                                "FROM (" +
                                    "SELECT * " +
                                    "FROM %s t " +
                                    "ORDER BY ROWID" +
                                ") t" +
                                "WHERE rownum <= ?" +
                            ") " +
                            "WHERE rnum BETWEEN ? AND ?",
                            columnList,
                            table
                        );
                    }

                    System.out.println("Processing table: " + table + " (" + rowCount + " rows)");

                    // Tạo task cho từng chunk
                    for (int i = 0; i < config.getWorkers(); i++) {
                        long start = 1 + i * chunkSize;
                        long end = start + chunkSize - 1;
                        if (i == config.getWorkers() - 1) {
                            end = rowCount;
                        }

                        executor.submit(new CopyTask(
                                config.getSource(),
                                config.getTarget(),
                                table,
                                columns,
                                start,
                                end,
                                query,
                                oraclePool,
                                postgresPool
                        ));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            executor.shutdown();
            executor.awaitTermination(1, java.util.concurrent.TimeUnit.DAYS);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            long endTime = System.currentTimeMillis();
            System.out.println("Total execution time: " + (endTime - startTime) / 1000 + "s");
        }
    }

    // Phương thức tạo pool connection
    private static HikariDataSource createPool(DataSourceConfig config, int maxPoolSize) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(config.getUrl());
        hc.setUsername(config.getUsername());
        hc.setPassword(config.getPassword());
        hc.setMaximumPoolSize(maxPoolSize); // Số connection = 2 * số worker
        hc.setConnectionTimeout(30000);     // Thời gian chờ lấy connection (30s)
        return new HikariDataSource(hc);
    }
}