package cn.tenmg.clink.metadata.getter;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import cn.tenmg.clink.exception.IllegalConfigurationException;
import cn.tenmg.clink.metadata.MetaDataGetter.TableMetaData.ColumnType;
import cn.tenmg.clink.utils.JDBCUtils;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * Oracle元数据获取器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.1
 */
public class OracleMetaDataGetter extends JDBCMetaDataGetter {
	@Override
	public TableMetaData getTableMetaData(Map<String, String> dataSource, String tableName) throws Exception {
		Connection con = null;
		try {
			con = getConnection(dataSource);// 获得数据库连接
			con.setAutoCommit(true);
			DatabaseMetaData metaData = con.getMetaData();
			String catalog = con.getCatalog(), schema = con.getSchema(), parts[] = tableName.split("\\.", 2);
			if (parts.length > 1) {
				schema = parts[0];
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
