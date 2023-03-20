package cn.tenmg.flink.jobs.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 结果获取器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public interface ResultGetter<T> {
	/**
	 * 获取结果类型
	 * 
	 * @return 结果类型
	 */
	Class<T> getType();

	/**
	 * 获取结果值
	 * 
	 * @param rs
	 *            结果集
	 * @param columnIndex
	 *            列索引
	 * @return 结果值
	 * @throws SQLException
	 *             SQL异常
	 */
	T getValue(ResultSet rs, int columnIndex) throws SQLException;

	/**
	 * 获取结果值
	 * 
	 * @param rs
	 *            结果集
	 * @param columnLabel
	 *            列名
	 * @return 结果值
	 * @throws SQLException
	 *             SQL异常
	 */
	T getValue(ResultSet rs, String columnLabel) throws SQLException;
}
