package cn.tenmg.flink.jobs.operator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.Script;
import cn.tenmg.dsl.parser.JDBCParamsParser;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
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

	@Override
	public Object execute(StreamExecutionEnvironment env, Jdbc jdbc, Map<String, Object> params) throws Exception {
		NamedScript namedScript = DSLUtils.parse(jdbc.getScript(), params);
		String datasource = jdbc.getDataSource();
		Script<List<Object>> sql = DSLUtils.toScript(namedScript.getScript(), namedScript.getParams(),
				JDBCParamsParser.getInstance());
		if (StringUtils.isNotBlank(datasource)) {
			Map<String, String> dataSource = FlinkJobsContext.getDatasource(datasource);
			Connection con = null;
			PreparedStatement ps = null;
			try {
				con = JDBCUtils.getConnection(dataSource);// 获得数据库连接
				con.setAutoCommit(true);
				String statement = sql.getValue();
				ps = con.prepareStatement(statement);
				List<Object> paramters = sql.getParams();
				JDBCUtils.setParams(ps, paramters);

				System.out.println(String.format("Execute JDBC SQL: %s; parameters: %s", statement,
						JSONUtils.toJSONString(params)));
				String method = jdbc.getMethod();
				if ("executeLargeUpdate".equals(method)) {
					return ps.executeLargeUpdate();
				} else if ("executeUpdate".equals(method)) {
					return ps.executeUpdate();
				} else if ("execute".equals(method)) {
					return ps.execute();
				}
				return ps.executeLargeUpdate();
			} catch (Exception e) {
				throw e;
			} finally {
				JDBCUtils.close(ps);
				JDBCUtils.close(con);
			}
		} else {
			throw new IllegalArgumentException("dataSource must be not null");
		}
	}
}
