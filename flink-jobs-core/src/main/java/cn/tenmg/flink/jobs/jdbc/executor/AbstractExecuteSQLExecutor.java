package cn.tenmg.flink.jobs.jdbc.executor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import cn.tenmg.flink.jobs.jdbc.SQLExecutor;

/**
 * 抽象执行类SQL执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @param <T>
 *            返回结果类型
 *             
 * @since 1.4.0
 */
public abstract class AbstractExecuteSQLExecutor<T> implements SQLExecutor<T> {

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public ResultSet executeQuery(PreparedStatement ps) throws SQLException {
		return null;
	}

}
