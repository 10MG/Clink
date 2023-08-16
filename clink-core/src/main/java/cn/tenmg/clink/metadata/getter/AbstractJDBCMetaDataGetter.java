package cn.tenmg.clink.metadata.getter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MapUtils;

import cn.tenmg.clink.exception.IllegalConfigurationException;
import cn.tenmg.clink.metadata.MetaDataGetter;
import cn.tenmg.clink.metadata.MetaDataGetter.TableMetaData.ColumnType;
import cn.tenmg.clink.utils.JDBCUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * JDBC元数据获取器抽象类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.3
 * 
 */
public abstract class AbstractJDBCMetaDataGetter implements MetaDataGetter {

	protected static final String COLUMN_NAME = "COLUMN_NAME", DATA_TYPE = "DATA_TYPE", COLUMN_SIZE = "COLUMN_SIZE",
			DECIMAL_DIGITS = "DECIMAL_DIGITS", IS_NULLABLE = "IS_NULLABLE", NO = "NO";

	/**
	 * 根据数据源配置获取数据库连接
	 * 
	 * @param dataSource
	 *            数据源
	 * @return 返回数据库连接
	 */
	protected Connection getConnection(Map<String, String> dataSource) throws Exception {
		return JDBCUtils.getConnection(dataSource);
	}

	/**
	 * 获取数据表的主键列名集
	 * 
	 * @param con
	 *            连接
	 * @param catalog
	 *            目录
	 * @param schema
	 *            数据库实例
	 * @param tableName
	 *            表名
	 * @return 主键列名集
	 * @throws SQLException
	 *             执行发生SQL异常
	 */
	abstract Set<String> getPrimaryKeys(Connection con, String catalog, String schema, String tableName)
			throws SQLException;

	@Override
	public TableMetaData getTableMetaData(Map<String, String> dataSource, String tableName) throws Exception {
		Connection con = null;
		try {
			con = getConnection(dataSource);// 获得数据库连接
			con.setAutoCommit(true);
			DatabaseMetaData metaData = con.getMetaData();
			String catalog = con.getCatalog(), schema = con.getSchema(), parts[] = tableName.split("\\.", 2);
			if (parts.length > 1) {
				catalog = parts[0];
				tableName = parts[1];
			}
			Set<String> primaryKeys = getPrimaryKeys(con, catalog, schema, tableName);
			ResultSet columnsSet = metaData.getColumns(catalog, schema, tableName, null);
			String columnName;
			Map<String, ColumnType> columns = new LinkedHashMap<String, ColumnType>();
			while (columnsSet.next()) {
				columnName = columnsSet.getString(COLUMN_NAME);
				columns.put(columnName,
						ColumnType.builder().typeName(columnsSet.getString("TYPE_NAME").toUpperCase())
								.dataType(columnsSet.getInt(DATA_TYPE)).scale(columnsSet.getInt(DECIMAL_DIGITS))
								.precision(columnsSet.getInt(COLUMN_SIZE))
								.isNotNull(NO.equals(columnsSet.getString(IS_NULLABLE))).build());
			}
			if (MapUtils.isEmpty(columns)) {
				throw new IllegalConfigurationException(
						StringUtils.concat("Table ", catalog, ".", tableName, " not found"));
			}
			return new TableMetaData(primaryKeys, columns);
		} finally {
			JDBCUtils.close(con);
		}
	}

}
