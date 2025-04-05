package cn.tenmg.clink.jdbc.executer;

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
public class ExecuteLargeUpdateSQLExecuter extends AbstractExecuteSQLExecuter<Long> {

	private static final ExecuteLargeUpdateSQLExecuter INSTANCE = new ExecuteLargeUpdateSQLExecuter();

	private ExecuteLargeUpdateSQLExecuter() {
		super();
	}

	public static final ExecuteLargeUpdateSQLExecuter getInstance() {
		return INSTANCE;
	}

	@Override
	public Long execute(PreparedStatement ps, ResultSet rs) throws SQLException {
		return ps.executeLargeUpdate();
	}

}
