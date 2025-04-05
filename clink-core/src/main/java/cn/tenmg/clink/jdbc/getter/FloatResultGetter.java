package cn.tenmg.clink.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link java.lang.Float} 类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class FloatResultGetter extends AbstractResultGetter<Float> {

	@Override
	public Float getValue(ResultSet rs, int columnIndex) throws SQLException {
		return toFloat(rs.getObject(columnIndex));
	}

	@Override
	public Float getValue(ResultSet rs, String columnLabel) throws SQLException {
		return toFloat(rs.getObject(columnLabel));
	}

	private static Float toFloat(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Float) {
			return (Float) value;
		} else {
			return Float.valueOf(value.toString());
		}
	}

}
