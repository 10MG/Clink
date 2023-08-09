package cn.tenmg.clink.metadata.getter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.clink.utils.JDBCUtils;

/**
 * StarRocks元数据获取器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.3
 */
public class StarrocksMetaDataGetter extends AbstractJDBCMetaDataGetter {

	private static final boolean UK_AS_PK = Boolean
			.valueOf(ClinkContext.getProperty(Arrays.asList("metadata.starrocks.unique_key_as_primary_key",
					"metadata.starrocks.unique-key-as-primary-key"), "true")),
			CAL_AS_SCM = Boolean.valueOf(ClinkContext.getProperty(
					Arrays.asList("metadata.starrocks.catalog_as_schema", "metadata.starrocks.catalog-as-schema"),
					"true"));// 兼容老的配置

	@Override
	protected Set<String> getPrimaryKeys(Connection con, String catalog, String schema, String tableName)
			throws SQLException {
		StringBuilder sqlBuilder = new StringBuilder(
				"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_KEY "
						+ (UK_AS_PK ? "IN ('PRI','UNI')" : "= 'PRI'"));
		// StarRocks JDBC适配有问题，catalog和schema对调了（catalog本应为null，但实际上却是schema的值）
		// 因此这里允许用户选择是否将catalog作为schema
		if (schema != null || (CAL_AS_SCM && catalog != null)) {
			sqlBuilder.append(" AND TABLE_SCHEMA = ?");
		}
		sqlBuilder.append(" AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION");
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement(sqlBuilder.toString());
			int nextId = 1;
			if (schema != null) {
				ps.setString(nextId++, schema);
			} else if (CAL_AS_SCM && catalog != null) {
				ps.setString(nextId++, catalog);
			}
			ps.setString(nextId, tableName);
			rs = ps.executeQuery();
			Set<String> primaryKeys = new LinkedHashSet<String>();
			while (rs.next()) {
				primaryKeys.add(rs.getString(COLUMN_NAME));
			}
			return primaryKeys;
		} finally {
			JDBCUtils.close(rs);
			JDBCUtils.close(ps);
		}
	}

}
