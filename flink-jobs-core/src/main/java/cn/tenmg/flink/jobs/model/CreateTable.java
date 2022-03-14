package cn.tenmg.flink.jobs.model;

/**
 *
 */
public class CreateTable extends SqlQuery {

    private String dataSource;

    private String tableName;

    /**
     *
     * @return
     */
    public String getDataSource() {
        return dataSource;
    }

    /**
     *
     * @param dataSource
     */
    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
