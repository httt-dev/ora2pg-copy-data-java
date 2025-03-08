package com.example.migration.config;

import java.util.List;

public class Config {
    private DataSourceConfig source;
    private DataSourceConfig target;
    private List<String> tables;
    private int workers;

    // Getters and Setters
    public DataSourceConfig getSource() { return source; }
    public void setSource(DataSourceConfig source) { this.source = source; }

    public DataSourceConfig getTarget() { return target; }
    public void setTarget(DataSourceConfig target) { this.target = target; }

    public List<String> getTables() { return tables; }
    public void setTables(List<String> tables) { this.tables = tables; }

    public int getWorkers() { return workers; }
    public void setWorkers(int workers) { this.workers = workers; }
}