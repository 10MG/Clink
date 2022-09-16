package cn.tenmg.flink.jobs.jdbc.executor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 执行大数据量更新的SQL执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.4.0
 */
public class ExecuteLargeUpdateSQLExecutor extends AbstractExecuteSQLExecutor<Long> {

	private static final ExecuteLargeUpdateSQLExecutor INSTANCE = new ExecuteLargeUpdateSQLExecutor();

	private ExecuteLargeUpdateSQLExecutor() {
		super();
	}

	public static final ExecuteLargeUpdateSQLExecutor getInstance() {
		return INSTANCE;
	}

	@Override
	public Long execute(PreparedStatement ps, ResultSet rs) throws SQLException {
		return ps.executeLargeUpdate();
	}

}
