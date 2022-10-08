package cn.tenmg.flink.jobs.operator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.Script;
import cn.tenmg.dsl.parser.JDBCParamsParser;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.jdbc.SQLExecutor;
import cn.tenmg.flink.jobs.jdbc.executor.ExecuteLargeUpdateSQLExecutor;
import cn.tenmg.flink.jobs.jdbc.executor.ExecuteSQLExecutor;
import cn.tenmg.flink.jobs.jdbc.executor.ExecuteUpdateSQLExecutor;
import cn.tenmg.flink.jobs.jdbc.executor.GetSQLExecutor;
import cn.tenmg.flink.jobs.jdbc.executor.SelectSQLExecutor;
import cn.tenmg.flink.jobs.model.Jdbc;
import cn.tenmg.flink.jobs.utils.JDBCUtils;
import cn.tenmg.flink.jobs.utils.JSONUtils;

/**
 * JBDC操作执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.1.1
 */
public class JdbcOperator extends AbstractOperator<Jdbc> {

	private static Logger log = LoggerFactory.getLogger(JdbcOperator.class);

	private static Map<String, SQLExecutor<?>> SQLExecuters = new HashMap<String, SQLExecutor<?>>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2696116935428505003L;

		{
			put("executeLargeUpdate", ExecuteLargeUpdateSQLExecutor.getInstance());
			put("executeUpdate", ExecuteUpdateSQLExecutor.getInstance());
			put("execute", ExecuteSQLExecutor.getInstance());
		}
	};

	private static Set<String> SQLExecuterKeys = new HashSet<String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 2825056328562857566L;
		{
			addAll(SQLExecuters.keySet());
			add("get");
			add("select");
		}

	};

	@Override
	public Object execute(StreamExecutionEnvironment env, Jdbc jdbc, Map<String, Object> params) throws Exception {
		NamedScript namedScript = DSLUtils.parse(jdbc.getScript(), params);
		String datasource = jdbc.getDataSource(), script = namedScript.getScript();
		Map<String, Object> usedParams = namedScript.getParams();
		Script<List<Object>> sql = DSLUtils.toScript(script, usedParams, JDBCParamsParser.getInstance());
		if (StringUtils.isNotBlank(datasource)) {
			log.info(String.format("Execute JDBC SQL: %s; parameters: %s", script, JSONUtils.toJSONString(usedParams)));
			String method = jdbc.getMethod();
			if (!SQLExecuterKeys.contains(method)) {
				method = FlinkJobsContext.getProperty("jdbc.default_method", "execute");
			}
			SQLExecutor<?> executer = SQLExecuters.get(method);
			if (executer == null) {
				if ("get".equals(method)) {
					String resultClass = jdbc.getResultClass();
					if (StringUtils.isBlank(resultClass)) {
						executer = new GetSQLExecutor<>(Object.class);
					} else {
						executer = new GetSQLExecutor<>(Class.forName(resultClass));
					}
				} else {
					String resultClass = jdbc.getResultClass();
					if (StringUtils.isBlank(resultClass)) {
						executer = new SelectSQLExecutor<>(HashMap.class);
					} else {
						executer = new SelectSQLExecutor<>(Class.forName(resultClass));
					}
				}
			}
			return execute(datasource, sql.getValue(), sql.getParams(), executer);
		} else {
			throw new IllegalArgumentException("dataSource must be not null");
		}
	}

	private <T> T execute(String datasource, String sql, List<Object> params, SQLExecutor<T> sqlExecuter)
			throws SQLException, ClassNotFoundException {
		Connection con = null;
		T result = null;
		try {
			con = JDBCUtils.getConnection(datasource);// 获得数据库连接
			con.setAutoCommit(true);
			con.setReadOnly(sqlExecuter.isReadOnly());
			result = execute(con, sql, params, sqlExecuter);
		} finally {
			JDBCUtils.close(con);
		}
		return result;
	}

	/**
	 * 执行一个SQL语句
	 * 
	 * @param con
	 *            连接对象
	 * @param sql
	 *            SQL语句
	 * @param params
	 *            参数
	 * @param sqlExecuter
	 *            SQL执行器
	 * @return 返回执行SQL的返回值
	 * @throws SQLException
	 *             SQL异常
	 */
	public static <T> T execute(Connection con, String sql, List<Object> params, SQLExecutor<T> sqlExecuter)
			throws SQLException {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = con.prepareStatement(sql);
			JDBCUtils.setParams(ps, params);
			return sqlExecuter.execute(ps, rs = sqlExecuter.executeQuery(ps));
		} finally {
			JDBCUtils.close(rs);
			JDBCUtils.close(ps);
		}
	}
}
