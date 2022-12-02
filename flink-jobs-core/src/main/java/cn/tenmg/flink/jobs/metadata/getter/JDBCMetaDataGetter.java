package cn.tenmg.flink.jobs.metadata.getter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * JDBC元数据获取器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public class JDBCMetaDataGetter extends AbstractJDBCMetaDataGetter {

	@Override
	Set<String> getPrimaryKeys(Connection con, String catalog, String schema, String tableName)
			throws SQLException {
		ResultSet primaryKeysSet = con.getMetaData().getPrimaryKeys(catalog, schema, tableName);
		Set<String> primaryKeys = new HashSet<String>();
		while (primaryKeysSet.next()) {
			primaryKeys.add(primaryKeysSet.getString(COLUMN_NAME));
		}
		return primaryKeys;
	}

}