package cn.tenmg.flink.jobs.jdbc.executor;

import java.lang.reflect.ParameterizedType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import cn.tenmg.flink.jobs.jdbc.exception.SQLExecutorException;

/**
 * 查询单条记录的数据的SQL执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @param <T>
 *            实体类
 *
 * @since 1.4.0
 */
public class GetSQLExecutor<T> extends ReadOnlySQLExecutor<T> {

	protected Class<T> type;

	@SuppressWarnings("unchecked")
	protected GetSQLExecutor() {
		type = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
	}

	public GetSQLExecutor(Class<T> type) {
		this.type = type;
	}

	@Override
	public boolean isReadOnly() {
		return true;
	}

	@Override
	public ResultSet executeQuery(PreparedStatement ps) throws SQLException {
		return ps.executeQuery();
	}

	@Override
	public T execute(PreparedStatement ps, ResultSet rs) throws SQLException {
		T row = null;
		if (rs.next()) {
			row = getRow(rs, type);
			if (rs.next()) {
				throw new SQLExecutorException(
						"Statement returned more than one row, where no more than one was expected.");
			}
		}
		return row;
	}

}
