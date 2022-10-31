package cn.tenmg.flink.jobs.model;

import java.util.List;

import cn.tenmg.flink.jobs.model.create.table.Column;

/**
 * Flink SQL的createTable操作配置
 *
 * @author dufeng
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.3.0
 *
 */
public class CreateTable extends Operate {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4209861215644827005L;

	private String catalog;

	private String dataSource;

	private String dataSourceFilter;

	private String tableName;

	private String bindTableName;

	private String primaryKey;

	private Boolean smart;

	private List<Column> columns;

	/**
	 * 获取使用的目录
	 * 
	 * @return 使用的目录
	 */
	public String getCatalog() {
		return catalog;
	}

	/**
	 * 设置使用的目录
	 * 
	 * @param catalog
	 *            使用的目录
	 */
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}

	/**
	 * 获取使用的数据源名称
	 * 
	 * @return 使用的数据源名称
	 */
	public String getDataSource() {
		return dataSource;
	}

	/**
	 * 设置使用的数据源名称
	 * 
	 * @param dataSource
	 *            使用的数据源名称
	 */
	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	/**
	 * 获取数据源过滤器
	 * 
	 * @return 数据源过滤器
	 */
	public String getDataSourceFilter() {
		return dataSourceFilter;
	}

	/**
	 * 设置数据源过滤器
	 * 
	 * @param dataSourceFilter
	 *            数据源过滤器
	 */
	public void setDataSourceFilter(String dataSourceFilter) {
		this.dataSourceFilter = dataSourceFilter;
	}

	/**
	 * 获取创建的表名
	 * 
	 * @return 创建的表名
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * 设置创建的表名
	 * 
	 * @param tableName
	 *            创建的表名
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * 获取绑定的表名，即WITH子句的“table-name”
	 * 
	 * @return 返回绑定的表名
	 */
	public String getBindTableName() {
		return bindTableName;
	}

	/**
	 * 设置绑定的表名，即WITH子句的“table-name”
	 * 
	 * @param bindTableName
	 *            绑定的表名，即WITH子句的“table-name”
	 */
	public void setBindTableName(String bindTableName) {
		this.bindTableName = bindTableName;
	}

	/**
	 * 获取主键
	 * 
	 * @return 返回主键
	 */
	public String getPrimaryKey() {
		return primaryKey;
	}

	/**
	 * 设置主键
	 * 
	 * @param primaryKey
	 *            主键。如为复合主键，多个字段名之间使用“,”分隔
	 */
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	/**
	 * 获取智能模式状态
	 * 
	 * @return 返回智能模式状态
	 */
	public Boolean getSmart() {
		return smart;
	}

	/**
	 * 设置智能模式状态。true表示开启智能模式，即自动查询列名和数据类型信息，false则表示仅使用指定的列执行数据同步，不设置表示使用配置文件的配置，如果配置文件未指定则默认为true。
	 * 
	 * @param smart
	 *            智能模式状态
	 */
	public void setSmart(Boolean smart) {
		this.smart = smart;
	}

	/**
	 * 获取创建表的数据列
	 * 
	 * @return 创建表的数据列
	 */
	public List<Column> getColumns() {
		return columns;
	}

	/**
	 * 设置创建表的数据列
	 * 
	 * @param columns
	 *            创建表的数据列
	 */
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

}
