package cn.tenmg.flink.jobs.jdbc.executor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 执行SQL的执行器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.4.0
 */
public class ExecuteSQLExecutor extends AbstractExecuteSQLExecutor<Boolean> {

	private static final ExecuteSQLExecutor INSTANCE = new ExecuteSQLExecutor();

	private ExecuteSQLExecutor() {
		super();
	}

	public static final ExecuteSQLExecutor getInstance() {
		return INSTANCE;
	}

	@Override
	public ResultSet executeQuery(PreparedStatement ps) throws SQLException {
		return null;
	}

	@Override
	public Boolean execute(PreparedStatement ps, ResultSet rs) throws SQLException {
		return ps.execute();
	}

}
