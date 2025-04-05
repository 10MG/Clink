package cn.tenmg.clink.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * SQL 执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @param <T>
 *            实体类
 * @since 1.4.0
 */
public interface SQLExecuter<T> {

	boolean isReadOnly();

	ResultSet executeQuery(PreparedStatement ps) throws SQLException;

	T execute(PreparedStatement ps, ResultSet rs) throws SQLException;
}
