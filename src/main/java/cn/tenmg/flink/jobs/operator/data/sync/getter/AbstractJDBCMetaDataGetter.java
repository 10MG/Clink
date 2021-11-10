package cn.tenmg.flink.jobs.operator.data.sync.getter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetter;
import cn.tenmg.flink.jobs.utils.JDBCUtils;

/**
 * JDBC元数据获取器抽象类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.3
 */
public abstract class AbstractJDBCMetaDataGetter implements MetaDataGetter {

	private static final String COLUMN_NAME = "COLUMN_NAME", DATA_TYPE = "DATA_TYPE", COLUMN_SIZE = "COLUMN_SIZE",
			DECIMAL_DIGITS = "DECIMAL_DIGITS", IS_NULLABLE = "IS_NULLABLE", NO = "NO";

	/**
	 * 根据数据源配置获取数据库连接
	 * 
	 * @param dataSource
	 *            数据源
	 * @return 返回数据库连接
	 */
	abstract Connection getConnection(Map<String, String> dataSource) throws Exception;

	@Override
	public TableMetaData getTableMetaData(Map<String, String> dataSource, String tableName) throws Exception {
		Connection con = null;
		try {
			con = getConnection(dataSource);// 获得数据库连接
			con.setAutoCommit(true);
			DatabaseMetaData metaData = con.getMetaData();
			String catalog = con.getCatalog(), schema = con.getSchema();

			ResultSet primaryKeysSet = metaData.getPrimaryKeys(catalog, schema, tableName);
			Set<String> primaryKeys = new HashSet<String>();
			while (primaryKeysSet.next()) {
				primaryKeys.add(primaryKeysSet.getString(COLUMN_NAME));
			}

			ResultSet columnsSet = metaData.getColumns(catalog, schema, tableName, null);
			int dataType;
			String columnName, type;
			Map<String, String> columns = new LinkedHashMap<String, String>();
			while (columnsSet.next()) {
				columnName = columnsSet.getString(COLUMN_NAME);
				dataType = columnsSet.getInt(DATA_TYPE);
				type = getType(dataType, columnsSet);
				if (NO.equals(columnsSet.getString(IS_NULLABLE))) {
					type += " NOT NULL";
				}
				columns.put(columnName, type);
			}
			return new TableMetaData(primaryKeys, columns);
		} catch (Exception e) {
			throw e;
		} finally {
			JDBCUtils.close(con);
		}
	}

	private static String getType(int dataType, ResultSet columnsSet) throws SQLException {
		int columnSize;
		switch (dataType) {
		case java.sql.Types.VARCHAR:
		case java.sql.Types.CHAR:
		case java.sql.Types.NVARCHAR:
		case java.sql.Types.NCHAR:
		case java.sql.Types.LONGNVARCHAR:
		case java.sql.Types.LONGVARCHAR:
			return "STRING";
		case java.sql.Types.BIGINT:
			return "BIGINT";
		case java.sql.Types.BOOLEAN:
			return "BOOLEAN";
		case java.sql.Types.BIT:
			if (columnsSet.getInt(COLUMN_SIZE) == 1) {
				return "BOOLEAN";
			} else {
				return "TINYINT";
			}
		case java.sql.Types.DECIMAL:
			return "DECIMAL(" + columnsSet.getInt(COLUMN_SIZE) + "," + columnsSet.getInt(DECIMAL_DIGITS) + ")";
		case java.sql.Types.DOUBLE:
			return "DOUBLE";
		case java.sql.Types.FLOAT:
		case java.sql.Types.REAL:
			return "FLOAT";
		case java.sql.Types.INTEGER:
			return "INT";
		case java.sql.Types.NUMERIC:
			return "NUMERIC(" + columnsSet.getInt(COLUMN_SIZE) + "," + columnsSet.getInt(DECIMAL_DIGITS) + ")";
		case java.sql.Types.SMALLINT:
			return "SMALLINT";
		case java.sql.Types.TINYINT:
			return "TINYINT";
		case java.sql.Types.DATE:
			return "DATE";
		case java.sql.Types.TIME:
		case java.sql.Types.TIME_WITH_TIMEZONE:
			columnSize = columnsSet.getInt(COLUMN_SIZE);
			if (columnSize > 8) {
				return "TIME(" + (columnSize - 9) + ")";
			} else {
				return "TIME";
			}
		case java.sql.Types.TIMESTAMP:
		case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:
			columnSize = columnsSet.getInt(COLUMN_SIZE);
			if (columnSize > 19) {
				return "TIMESTAMP(" + (columnSize - 20) + ")";
			} else {
				return "TIMESTAMP";
			}
		case java.sql.Types.BINARY:
		case java.sql.Types.LONGVARBINARY:
		case java.sql.Types.VARBINARY:
			return "BYTES";
		case java.sql.Types.REF:
			return "REF";
		case java.sql.Types.DATALINK:
			return "DATALINK";
		case java.sql.Types.ARRAY:
			return "ARRAY";
		case java.sql.Types.BLOB:
			return "BLOB";
		case java.sql.Types.CLOB:
		case java.sql.Types.NCLOB:
			return "CLOB";
		case java.sql.Types.STRUCT:
			return "STRUCT";
		default:
			return "STRING";
		}
	}

}
