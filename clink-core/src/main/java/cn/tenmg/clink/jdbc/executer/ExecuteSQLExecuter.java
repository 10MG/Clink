package cn.tenmg.clink.jdbc.executer;

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
public class ExecuteSQLExecuter extends AbstractExecuteSQLExecuter<Boolean> {

	private static final ExecuteSQLExecuter INSTANCE = new ExecuteSQLExecuter();

	private ExecuteSQLExecuter() {
		super();
	}

	public static final ExecuteSQLExecuter getInstance() {
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
