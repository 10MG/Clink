package cn.tenmg.clink.metadata;

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

		private Map<String, ColumnType> columns;

		public Set<String> getPrimaryKeys() {
			return primaryKeys;
		}

		public void setPrimaryKeys(Set<String> primaryKeys) {
			this.primaryKeys = primaryKeys;
		}

		public Map<String, ColumnType> getColumns() {
			return columns;
		}

		public void setColumns(Map<String, ColumnType> columns) {
			this.columns = columns;
		}

		public TableMetaData(Set<String> primaryKeys, Map<String, ColumnType> columns) {
			super();
			this.primaryKeys = primaryKeys;
			this.columns = columns;
		}

		/**
		 * 列数据类型
		 * 
		 * @author June wjzhao@aliyun.com
		 * 
		 * @since 1.6.0
		 */
		public static class ColumnType {

			private String typeName;

			private int scale;

			private int precision;

			private int dataType;

			private boolean notNull;

			/**
			 * 获取数据类型名
			 * 
			 * @return 数据类型名
			 */
			public String getTypeName() {
				return typeName;
			}

			/**
			 * 获取长度
			 * 
			 * @return 长度
			 */
			public int getScale() {
				return scale;
			}

			/**
			 * 获取精度
			 * 
			 * @return 精度
			 */
			public int getPrecision() {
				return precision;
			}

			/**
			 * 获取 JDBC 数据类型
			 * 
			 * @return JDBC 数据类型
			 */
			public int getDataType() {
				return dataType;
			}

			/**
			 * 判断该列是否不允许为 {@code null}
			 * 
			 * @return 如不允许为 {@code null}，则返回 {@code true}；否则，返回 {@code false}
			 */
			public boolean isNotNull() {
				return notNull;
			}

			/**
			 * 创建列类型构建器
			 * 
			 * @return 列类型构建器
			 */
			public static Builder builder() {
				return new Builder();
			}

			/**
			 * 列类型构建器
			 * 
			 * @author June wjzhao@aliyun.com
			 * 
			 * @since 1.6.0
			 */
			public static class Builder {

				private final ColumnType columnType = new ColumnType();

				private Builder() {
					super();
				}

				public Builder typeName(String typeName) {
					columnType.typeName = typeName;
					return this;
				}

				public Builder scale(int scale) {
					columnType.scale = scale;
					return this;
				}

				public Builder precision(int precision) {
					columnType.precision = precision;
					return this;
				}

				public Builder dataType(int dataType) {
					columnType.dataType = dataType;
					return this;
				}
				
				public Builder isNotNull(boolean notNull) {
					columnType.notNull = notNull;
					return this;
				}

				public ColumnType build() {
					return columnType;
				}
			}

		}

	}
}
