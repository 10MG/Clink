package cn.tenmg.flink.jobs.model;

/**
 * Flink SQL的createTable操作配置
 *
 * @author dufeng
 *
 * @since 1.3.0
 *
 */
public class CreateTable extends SqlQuery {

    /**
     * dataSource in flink-jobs.properties
     */
    private String dataSource;

    /**
     * created tableName
     */
    private String tableName;


    public String getDataSource() {
        return dataSource;
    }

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
