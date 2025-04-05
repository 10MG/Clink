package cn.tenmg.clink.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * java.lang.Long类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class LongResultGetter extends AbstractResultGetter<Long> {

	@Override
	public Long getValue(ResultSet rs, int columnIndex) throws SQLException {
		return toLong(rs.getObject(columnIndex));
	}

	@Override
	public Long getValue(ResultSet rs, String columnLabel) throws SQLException {
		return toLong(rs.getObject(columnLabel));
	}

	private static Long toLong(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Long) {
			return (Long) value;
		} else {
			return Long.valueOf(value.toString());
		}
	}
}
