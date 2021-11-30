package cn.tenmg.flink.jobs.operator.data.sync;

import java.util.Map;
import java.util.Set;

/**
 * 元数据获取器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public interface MetaDataGetter {

	/**
	 * 根据数据源、表名获取表元数据
	 * 
	 * @param dataSource
	 *            数据源
	 * @param tableName
	 *            表名
	 * @return 返回表元数据
	 * @throws Exception
	 *             发生异常
	 */
	TableMetaData getTableMetaData(Map<String, String> dataSource, String tableName) throws Exception;

	/**
	 * 
	 * 表元数据
	 * 
	 * @author June wjzhao@aliyun.com
	 * 
	 * @since 1.1.2
	 */
	public static class TableMetaData {

		private Set<String> primaryKeys;

		private Map<String, String> columns;

		public Set<String> getPrimaryKeys() {
			return primaryKeys;
		}

		public void setPrimaryKeys(Set<String> primaryKeys) {
			this.primaryKeys = primaryKeys;
		}

		public Map<String, String> getColumns() {
			return columns;
		}

		public void setColumns(Map<String, String> columns) {
			this.columns = columns;
		}

		public TableMetaData(Set<String> primaryKeys, Map<String, String> columns) {
			super();
			this.primaryKeys = primaryKeys;
			this.columns = columns;
		}

	}
}
