package com.example.migration;

import com.example.migration.config.Config;
import com.example.migration.config.DataSourceConfig;
import com.example.migration.task.CopyTask;
import com.example.migration.util.MetadataUtil;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataMigration {
    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Yaml yaml = new Yaml(new Constructor(Config.class));
        try (InputStream inputStream = DataMigration.class.getClassLoader().getResourceAsStream("config.yml")) {
            Config config = yaml.load(inputStream);

            try (Connection oracleConn = DriverManager.getConnection(
                    config.getSource().getUrl(),
                    config.getSource().getUsername(),
                    config.getSource().getPassword())) {

                ExecutorService executor = Executors.newFixedThreadPool(config.getWorkers());

                for (String table : config.getTables()) {
                    List<String> columns = MetadataUtil.getColumns(oracleConn, table);
                    String orderBy = MetadataUtil.getOrderByClause(oracleConn, table);
                    long rowCount = MetadataUtil.getRowCount(oracleConn, table);

                    // Xử lý trường hợp bảng rỗng
                    if (rowCount == 0) continue;

                    long chunkSize = rowCount / config.getWorkers();
                    chunkSize = Math.max(chunkSize, 1); // Tránh chia cho 0

                    String query;
                    if (MetadataUtil.hasPrimaryKey(oracleConn, table)) {
                        query = String.format(
                                "SELECT * FROM %s WHERE %s BETWEEN ? AND ?",
                                table, orderBy
                        );
                    } else {
                        String columnList = String.join(",", columns);
                        query = String.format(
                                "SELECT %s FROM (SELECT t.*, ROW_NUMBER() OVER (ORDER BY %s) rnum FROM %s t) WHERE rnum BETWEEN ? AND ?",
                                columnList, orderBy, table
                        );
                    }

                    List<MetadataUtil.Column> columnsWithTypes = MetadataUtil.getColumnsWithTypes(oracleConn, table);

                    for (int i = 0; i < config.getWorkers(); i++) {
                        long start = 1 + i * chunkSize;
                        long end = (i == config.getWorkers() - 1)
                                ? rowCount
                                : start + chunkSize - 1;

                        executor.submit(new CopyTask(
                                config.getSource(),
                                config.getTarget(),
                                table,
                                columnsWithTypes,
                                start,
                                end,
                                query
                        ));
                    }
                }

                executor.shutdown();
                executor.awaitTermination(1, java.util.concurrent.TimeUnit.DAYS);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            long endTime = System.currentTimeMillis();
            double durationInSeconds = (endTime - startTime) / 1000.0;
            System.out.println("Execution time: " + durationInSeconds + " seconds");
        }
    }
}