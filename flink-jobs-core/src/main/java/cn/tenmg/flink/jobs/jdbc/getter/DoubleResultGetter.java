package cn.tenmg.flink.jobs.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link java.lang.Double}类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class DoubleResultGetter extends AbstractResultGetter<Double> {

	@Override
	public Double getValue(ResultSet rs, int columnIndex) throws SQLException {
		return toDouble(rs.getObject(columnIndex));
	}

	@Override
	public Double getValue(ResultSet rs, String columnLabel) throws SQLException {
		return toDouble(rs.getObject(columnLabel));
	}

	private static Double toDouble(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Double) {
			return (Double) value;
		} else {
			return Double.valueOf(value.toString());
		}
	}
}
