package cn.tenmg.clink.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * java.lang.Short类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class ShortResultGetter extends AbstractResultGetter<Short> {

	@Override
	public Short getValue(ResultSet rs, int columnIndex) throws SQLException {
		return toShort(rs.getObject(columnIndex));
	}

	@Override
	public Short getValue(ResultSet rs, String columnLabel) throws SQLException {
		return toShort(rs.getObject(columnLabel));
	}

	private static Short toShort(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Short) {
			return (Short) value;
		} else {
			return Short.valueOf(value.toString());
		}
	}
}
