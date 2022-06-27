package cn.tenmg.flink.jobs.metadata.getter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.kit.HashMapKit;
import cn.tenmg.flink.jobs.kit.ParamsKit;
import cn.tenmg.flink.jobs.metadata.MetaDataGetter;
import cn.tenmg.flink.jobs.utils.JDBCUtils;

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
			DECIMAL_DIGITS = "DECIMAL_DIGITS", IS_NULLABLE = "IS_NULLABLE", NO = "NO", LEFT_BRACKET = "(",
			RIGTH_BRACKET = ")", TYPE_PREFFIX = "flink.sql.type" + FlinkJobsContext.CONFIG_SPLITER,
			DEFAULT_TYPE = FlinkJobsContext.getProperty(TYPE_PREFFIX + "default"),
			SIZE_OFFSET_SUFFIX = FlinkJobsContext.CONFIG_SPLITER + "size_offset";

	private static final Map<Integer, String> SQL_TYPES = HashMapKit
			.init(java.sql.Types.VARCHAR, "java.sql.Types.VARCHAR")
			.put(java.sql.Types.VARCHAR, "java.sql.Types.VARCHAR").put(java.sql.Types.CHAR, "java.sql.Types.CHAR")
			.put(java.sql.Types.NVARCHAR, "java.sql.Types.NVARCHAR").put(java.sql.Types.NCHAR, "java.sql.Types.NCHAR")
			.put(java.sql.Types.LONGNVARCHAR, "java.sql.Types.LONGNVARCHAR")
			.put(java.sql.Types.LONGVARCHAR, "java.sql.Types.LONGVARCHAR")
			.put(java.sql.Types.BIGINT, "java.sql.Types.BIGINT").put(java.sql.Types.BOOLEAN, "java.sql.Types.BOOLEAN")
			.put(java.sql.Types.BIT, "java.sql.Types.BIT").put(java.sql.Types.DECIMAL, "java.sql.Types.DECIMAL")
			.put(java.sql.Types.OTHER, "java.sql.Types.OTHER").put(java.sql.Types.DOUBLE, "java.sql.Types.DOUBLE")
			.put(java.sql.Types.FLOAT, "java.sql.Types.FLOAT").put(java.sql.Types.REAL, "java.sql.Types.REAL")
			.put(java.sql.Types.INTEGER, "java.sql.Types.INTEGER").put(java.sql.Types.NUMERIC, "java.sql.Types.NUMERIC")
			.put(java.sql.Types.SMALLINT, "java.sql.Types.SMALLINT")
			.put(java.sql.Types.TINYINT, "java.sql.Types.TINYINT").put(java.sql.Types.DATE, "java.sql.Types.DATE")
			.put(java.sql.Types.TIME, "java.sql.Types.TIME")
			.put(java.sql.Types.TIME_WITH_TIMEZONE, "java.sql.Types.TIME_WITH_TIMEZONE")
			.put(java.sql.Types.TIMESTAMP, "java.sql.Types.TIMESTAMP")
			.put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, "java.sql.Types.TIMESTAMP_WITH_TIMEZONE")
			.put(java.sql.Types.BINARY, "java.sql.Types.BINARY")
			.put(java.sql.Types.LONGVARBINARY, "java.sql.Types.LONGVARBINARY")
			.put(java.sql.Types.VARBINARY, "java.sql.Types.VARBINARY").put(java.sql.Types.REF, "java.sql.Types.REF")
			.put(java.sql.Types.DATALINK, "java.sql.Types.DATALINK").put(java.sql.Types.ARRAY, "java.sql.Types.ARRAY")
			.put(java.sql.Types.BLOB, "java.sql.Types.BLOB").put(java.sql.Types.CLOB, "java.sql.Types.CLOB")
			.put(java.sql.Types.NCLOB, "java.sql.Types.NCLOB").put(java.sql.Types.STRUCT, "java.sql.Types.STRUCT")
			.get();

	private static Set<String> WITH_PRECISION = asSafeSet(
			FlinkJobsContext.getProperty("flink.sql.type.with_precision")),
			WITH_SIZE = asSafeSet(FlinkJobsContext.getProperty("flink.sql.type.with_size"));

	/**
	 * 根据数据源配置获取数据库连接
	 * 
	 * @param dataSource
	 *            数据源
	 * @return 返回数据库连接
	 */
	abstract Connection getConnection(Map<String, String> dataSource) throws Exception;

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
	protected Set<String> getPrimaryKeys(Connection con, String catalog, String schema, String tableName)
			throws SQLException {
		ResultSet primaryKeysSet = con.getMetaData().getPrimaryKeys(catalog, schema, tableName);
		Set<String> primaryKeys = new HashSet<String>();
		while (primaryKeysSet.next()) {
			primaryKeys.add(primaryKeysSet.getString(COLUMN_NAME));
		}
		return primaryKeys;
	}

	@Override
	public TableMetaData getTableMetaData(Map<String, String> dataSource, String tableName) throws Exception {
		Connection con = null;
		try {
			con = getConnection(dataSource);// 获得数据库连接
			con.setAutoCommit(true);
			DatabaseMetaData metaData = con.getMetaData();
			String catalog = con.getCatalog(), schema = con.getSchema();
			Set<String> primaryKeys = getPrimaryKeys(con, catalog, schema, tableName);
			ResultSet columnsSet = metaData.getColumns(catalog, schema, tableName, null);
			String columnName, type;
			Map<String, String> columns = new LinkedHashMap<String, String>();
			while (columnsSet.next()) {
				columnName = columnsSet.getString(COLUMN_NAME);
				type = getType(dataSource, columnsSet.getInt(DATA_TYPE), columnsSet.getInt(COLUMN_SIZE),
						columnsSet.getInt(DECIMAL_DIGITS));
				if (NO.equals(columnsSet.getString(IS_NULLABLE))) {
					type += " NOT NULL";
				}
				columns.put(columnName, type);
			}
			return new TableMetaData(primaryKeys, columns);
		} finally {
			JDBCUtils.close(con);
		}
	}

	private static String getType(Map<String, String> dataSource, int dataType, int columnSize, int decimalDigits) {
		String sqlType = SQL_TYPES.get(dataType);
		String type = DEFAULT_TYPE;
		if (sqlType != null) {
			String connector = dataSource.get("connector");
			String possibleType = connector == null ? null : getSpecificProductType(connector.trim(), sqlType);// 获取特定连接器的类型映射配置，如starrocks
			if (StringUtils.isBlank(possibleType)) {// 否则，从url中获取产品名，获取特定JDBC产品的类型映射配置，如mysql
				String url = dataSource.get("url");
				if (StringUtils.isNotBlank(url)) {
					possibleType = getSpecificProductType(JDBCUtils.getProduct(url), sqlType);
				}
			}
			if (StringUtils.isBlank(possibleType)) {// 否则，获取全局配置的类型映射配置
				possibleType = FlinkJobsContext.getProperty(sqlType);
			}
			if (StringUtils.isBlank(possibleType)) {
				possibleType = FlinkJobsContext
						.getProperty(sqlType + LEFT_BRACKET + columnSize + "," + decimalDigits + RIGTH_BRACKET);
				if (StringUtils.isBlank(possibleType)) {
					possibleType = FlinkJobsContext.getProperty(sqlType + LEFT_BRACKET + columnSize + RIGTH_BRACKET);
					if (StringUtils.isNotBlank(possibleType)) {
						type = possibleType;
					}
				} else {
					type = possibleType;
				}
			} else {
				type = possibleType;
			}
		}
		return wrapType(type, columnSize, decimalDigits);
	}

	private static String getSpecificProductType(String product, String sqlType) {
		return FlinkJobsContext.getProperty(TYPE_PREFFIX + product + FlinkJobsContext.CONFIG_SPLITER + sqlType);
	}

	private static String wrapType(String possibleType, int columnSize, int decimalDigits) {
		possibleType = possibleType.trim();
		if (possibleType.endsWith(RIGTH_BRACKET)) {
			return DSLUtils
					.parse(possibleType,
							ParamsKit.init().put("columnSize", columnSize).put("decimalDigits", decimalDigits).get())
					.getScript();
		} else {
			if (WITH_PRECISION.contains(possibleType)) {// 类型含精度
				return possibleType + LEFT_BRACKET + columnSize + "," + decimalDigits + RIGTH_BRACKET;
			} else if (WITH_SIZE.contains(possibleType)) {// 类型含长度
				String sizeOffset = FlinkJobsContext.getProperty(TYPE_PREFFIX + possibleType + SIZE_OFFSET_SUFFIX);
				if (StringUtils.isBlank(sizeOffset)) {
					return possibleType + LEFT_BRACKET + columnSize + RIGTH_BRACKET;
				} else {
					int offset = Integer.parseInt(sizeOffset);
					if (columnSize >= offset) {
						return possibleType + LEFT_BRACKET + (columnSize - offset) + RIGTH_BRACKET;
					}
				}
			}
		}
		return possibleType;
	}

	private static final Set<String> asSafeSet(String string) {
		Set<String> set = new HashSet<String>();
		if (string != null) {
			String[] strings = string.split(",");
			for (int i = 0; i < strings.length; i++) {
				set.add(strings[i].trim());
			}
		}
		return set;
	}

}
