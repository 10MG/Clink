package cn.tenmg.flink.jobs.jdbc.executor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 执行更新的SQL执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.4.0
 */
public class ExecuteUpdateSQLExecutor extends AbstractExecuteSQLExecutor<Integer> {

	private static final ExecuteUpdateSQLExecutor INSTANCE = new ExecuteUpdateSQLExecutor();

	private ExecuteUpdateSQLExecutor() {
		super();
	}

	public static final ExecuteUpdateSQLExecutor getInstance() {
		return INSTANCE;
	}

	@Override
	public Integer execute(PreparedStatement ps, ResultSet rs) throws SQLException {
		return ps.executeUpdate();
	}

}
