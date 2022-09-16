package cn.tenmg.flink.jobs.jdbc.executor;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

import cn.tenmg.flink.jobs.jdbc.SQLExecutor;

/**
 * 只读SQL执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @param <T>
 *            返回结果类型
 * 
 * @since 1.4.0
 */
public abstract class ReadOnlySQLExecutor<T> implements SQLExecutor<T> {

	@Override
	public boolean isReadOnly() {
		return true;
	}

	/**
	 * 获取结果当前行集指定列的值
	 * 
	 * @param rs
	 *            结果集
	 * @param columnIndex
	 *            指定列索引
	 * @param type
	 *            值的类型
	 * @return 返回当前行集指定列的值
	 * @throws SQLException
	 */
	@SuppressWarnings("unchecked")
	protected static <T> T getValue(ResultSet rs, int columnIndex, Class<T> type) throws SQLException {
		if (BigDecimal.class.isAssignableFrom(type)) {
			return (T) rs.getBigDecimal(columnIndex);
		} else if (Number.class.isAssignableFrom(type)) {
			Object obj = rs.getObject(columnIndex);
			if (obj == null) {
				return null;
			}
			if (obj instanceof Number) {
				if (Double.class.isAssignableFrom(type)) {
					obj = ((Number) obj).doubleValue();
				} else if (Float.class.isAssignableFrom(type)) {
					obj = ((Number) obj).floatValue();
				} else if (Integer.class.isAssignableFrom(type)) {
					obj = ((Number) obj).intValue();
				} else if (Long.class.isAssignableFrom(type)) {
					obj = ((Number) obj).longValue();
				} else if (Short.class.isAssignableFrom(type)) {
					obj = ((Number) obj).shortValue();
				} else if (Byte.class.isAssignableFrom(type)) {
					obj = ((Number) obj).byteValue();
				}
			}
			return (T) obj;
		} else if (String.class.isAssignableFrom(type)) {
			return (T) rs.getString(columnIndex);
		}
		return (T) rs.getObject(columnIndex);
	}
}