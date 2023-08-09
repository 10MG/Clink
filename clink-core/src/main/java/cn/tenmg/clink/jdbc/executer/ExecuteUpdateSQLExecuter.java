package cn.tenmg.clink.jdbc.executer;

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
public class ExecuteUpdateSQLExecuter extends AbstractExecuteSQLExecuter<Integer> {

	private static final ExecuteUpdateSQLExecuter INSTANCE = new ExecuteUpdateSQLExecuter();

	private ExecuteUpdateSQLExecuter() {
		super();
	}

	public static final ExecuteUpdateSQLExecuter getInstance() {
		return INSTANCE;
	}

	@Override
	public Integer execute(PreparedStatement ps, ResultSet rs) throws SQLException {
		return ps.executeUpdate();
	}

}
