package cn.tenmg.clink.operator;

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

import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.clink.exception.IllegalJobConfigException;
import cn.tenmg.clink.jdbc.SQLExecuter;
import cn.tenmg.clink.jdbc.executer.ExecuteLargeUpdateSQLExecuter;
import cn.tenmg.clink.jdbc.executer.ExecuteSQLExecuter;
import cn.tenmg.clink.jdbc.executer.ExecuteUpdateSQLExecuter;
import cn.tenmg.clink.jdbc.executer.GetSQLExecuter;
import cn.tenmg.clink.jdbc.executer.ReadOnlySQLExecuter;
import cn.tenmg.clink.jdbc.executer.SelectSQLExecuter;
import cn.tenmg.clink.model.Jdbc;
import cn.tenmg.clink.utils.JDBCUtils;
import cn.tenmg.clink.utils.JSONUtils;
import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.Script;
import cn.tenmg.dsl.parser.JDBCParamsParser;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * JBDC操作执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.1.1
 */
public class JdbcOperator extends AbstractOperator<Jdbc> {

	private static Logger log = LoggerFactory.getLogger(JdbcOperator.class);

	private static Map<String, SQLExecuter<?>> sqlExecuters = new HashMap<String, SQLExecuter<?>>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2696116935428505003L;

		{
			put("executeLargeUpdate", ExecuteLargeUpdateSQLExecuter.getInstance());
			put("executeUpdate", ExecuteUpdateSQLExecuter.getInstance());
			put("execute", ExecuteSQLExecuter.getInstance());
		}
	};

	private static Map<String, ReadOnlySQLExecuterInfo> readOnlySQLExecutors = MapUtils
			.newHashMapBuilder(String.class, ReadOnlySQLExecuterInfo.class)
			.put("get", new ReadOnlySQLExecuterInfo(GetSQLExecuter.class, Object.class))
			.put("select", new ReadOnlySQLExecuterInfo(SelectSQLExecuter.class, HashMap.class)).build();

	private static Set<String> sqlExecuterKeys = new HashSet<String>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 2825056328562857566L;

		{
			addAll(sqlExecuters.keySet());
			addAll(readOnlySQLExecutors.keySet());
		}

	};

	@Override
	public Object execute(StreamExecutionEnvironment env, Jdbc jdbc, Map<String, Object> params) throws Exception {
		NamedScript namedScript = DSLUtils.parse(jdbc.getScript(), params);
		String datasource = jdbc.getDataSource(), script = namedScript.getScript();
		Map<String, Object> usedParams = namedScript.getParams();
		Script<List<Object>> sql = DSLUtils.toScript(script, usedParams, JDBCParamsParser.getInstance());
		if (StringUtils.isBlank(datasource)) {
			throw new IllegalJobConfigException("The property 'datasource' must be not blank");
		} else {
			String method = jdbc.getMethod();
			if (!sqlExecuterKeys.contains(method)) {
				method = ClinkContext.getProperty("jdbc.default-method", "execute");
			}
			SQLExecuter<?> executer = sqlExecuters.get(method);
			if (executer == null) {
				executer = getReadOnlySQLExecuter(method, jdbc.getResultClass());
			}
			Map<String, String> dataSource = ClinkContext.getDatasource(datasource);
			log.info(String.format("Execute JDBC SQL: %s; parameters: %s", script, JSONUtils.toJSONString(usedParams)));
			return execute(dataSource, sql.getValue(), sql.getParams(), executer);
		}
	}

	private ReadOnlySQLExecuter<?> getReadOnlySQLExecuter(String method, String resultClass) throws Exception {
		ReadOnlySQLExecuterInfo readOnlySQLExecuterInfo = readOnlySQLExecutors.get(method);
		Class<?> type = StringUtils.isBlank(resultClass) ? readOnlySQLExecuterInfo.getDefaultResultClass()
				: Class.forName(resultClass);
		return readOnlySQLExecuterInfo.getExecuterClass().getConstructor(Class.class).newInstance(type);
	}

	private <T> T execute(Map<String, String> dataSource, String sql, List<Object> params, SQLExecuter<T> sqlExecuter)
			throws SQLException, ClassNotFoundException {
		Connection con = null;
		T result = null;
		try {
			con = JDBCUtils.getConnection(dataSource);// 获得数据库连接
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
	public static <T> T execute(Connection con, String sql, List<Object> params, SQLExecuter<T> sqlExecuter)
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

	private static class ReadOnlySQLExecuterInfo {

		@SuppressWarnings("rawtypes")
		private Class<? extends ReadOnlySQLExecuter> executorClass;

		private Class<?> defaultResultClass;

		@SuppressWarnings("rawtypes")
		public Class<? extends ReadOnlySQLExecuter> getExecuterClass() {
			return executorClass;
		}

		public Class<?> getDefaultResultClass() {
			return defaultResultClass;
		}

		@SuppressWarnings("rawtypes")
		public ReadOnlySQLExecuterInfo(Class<? extends ReadOnlySQLExecuter> executorClass,
				Class<?> defaultResultClass) {
			this.executorClass = executorClass;
			this.defaultResultClass = defaultResultClass;
		}

	}

}
