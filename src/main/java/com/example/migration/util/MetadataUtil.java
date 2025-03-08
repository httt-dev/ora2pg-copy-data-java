package com.example.migration.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MetadataUtil {

    public static class Column {
        private String name;
        private String type;

        public Column(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public String getName() { return name; }
        public String getType() { return type; }
    }

    public static List<Column> getColumnsWithTypes(Connection conn, String table) throws SQLException {
        List<Column> columns = new ArrayList<>();
        String sql = "SELECT column_name, data_type FROM all_tab_columns WHERE table_name = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, table.toUpperCase());
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String name = rs.getString("column_name");
                String type = rs.getString("data_type");
                columns.add(new Column(name, type));
            }
        }
        return columns;
    }


    /**
     * Kiểm tra table có primary key hay không
     */
    public static boolean hasPrimaryKey(Connection conn, String table) throws SQLException {
        String sql = "SELECT COUNT(*) FROM all_constraints " +
                "WHERE table_name = ? AND constraint_type = 'P' AND owner = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, table.toUpperCase());
            pstmt.setString(2, conn.getSchema().toUpperCase());
            ResultSet rs = pstmt.executeQuery();
            return rs.next() && rs.getInt(1) > 0;
        }
    }

    /**
     * Lấy tên cột primary key (chỉ hỗ trợ single-column PK)
     */
    public static String getPrimaryKeyColumn(Connection conn, String table) throws SQLException {
        String sql = "SELECT cols.column_name " +
                "FROM all_constraints cons " +
                "JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name " +
                "WHERE cons.table_name = ? " +
                "AND cons.constraint_type = 'P' " +
                "AND cons.owner = ? " +
                "ORDER BY cols.position";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, table.toUpperCase());
            pstmt.setString(2, conn.getSchema().toUpperCase());
            ResultSet rs = pstmt.executeQuery();
            return rs.next() ? rs.getString("column_name") : null;
        }
    }

    /**
     * Lấy danh sách tất cả cột của table
     */
    public static List<String> getColumns(Connection conn, String table) throws SQLException {
        List<String> columns = new ArrayList<>();
        String sql = "SELECT column_name FROM all_tab_columns " +
                "WHERE table_name = ? AND owner = ? " +
                "ORDER BY column_id";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, table.toUpperCase());
            pstmt.setString(2, conn.getSchema().toUpperCase());
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                columns.add(rs.getString("column_name").toLowerCase());
            }
        }
        return columns;
    }

    /**
     * Lấy min/max của primary key
     */
    public static long[] getMinMaxPk(Connection conn, String table, String pkColumn) throws SQLException {
        String sql = String.format("SELECT MIN(%s) AS min_val, MAX(%s) AS max_val FROM %s",
                pkColumn, pkColumn, table);

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return new long[]{rs.getLong("min_val"), rs.getLong("max_val")};
            }
            throw new SQLException("Could not retrieve min/max for PK: " + pkColumn);
        }
    }

    /**
     * Lấy tổng số bản ghi trong table
     */
    public static long getRowCount(Connection conn, String table) throws SQLException {
        String sql = String.format("SELECT COUNT(*) FROM %s", table);
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            ResultSet rs = pstmt.executeQuery();
            return rs.next() ? rs.getLong(1) : 0;
        }
    }

    /**
     * Lấy danh sách các cột có thể dùng để phân trang (ưu tiên PK, sau đó là ROWID)
     */
    public static String getOrderByClause(Connection conn, String table) throws SQLException {
        if (hasPrimaryKey(conn, table)) {
            String pk = getPrimaryKeyColumn(conn, table);
            return pk != null ? pk : "ROWID";
        }
        return "ROWID";
    }
}