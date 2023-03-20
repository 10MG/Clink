package cn.tenmg.flink.jobs.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link java.lang.Integer} 类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class IntegerResultGetter extends AbstractResultGetter<Integer> {

	@Override
	public Integer getValue(ResultSet rs, int columnIndex) throws SQLException {
		return toInteger(rs.getObject(columnIndex));
	}

	@Override
	public Integer getValue(ResultSet rs, String columnLabel) throws SQLException {
		return toInteger(rs.getObject(columnLabel));
	}

	private static Integer toInteger(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Integer) {
			return (Integer) value;
		} else {
			return Integer.valueOf(value.toString());
		}
	}

}
