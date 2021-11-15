package cn.tenmg.flink.jobs.operator;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
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

/**
 * JBDC操作执行器
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 * @since 1.1.1
 */
public class JdbcOperator extends AbstractOperator<Jdbc> {

	@Override
	Object execute(StreamExecutionEnvironment env, Jdbc jdbc, Map<String, Object> params) throws Exception {
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
				
				StringBuilder sb = new StringBuilder();
				sb.append("Execute SQL: ").append(statement).append(", ").append("parameters: ")
						.append(toJSONString(params));
				System.out.println(sb.toString());

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

	/**
	 * 将参数集转化为JSON字符串
	 * 
	 * @param params
	 *            参数集
	 * @return 返回参数集的JSON字符串
	 */
	private static final String toJSONString(Collection<Object> params) {
		if (params != null) {
			StringBuilder sb = new StringBuilder("[");
			boolean flag = false;
			for (Iterator<Object> it = params.iterator(); it.hasNext();) {
				Object value = it.next();
				if (flag) {
					sb.append(", ");
				} else {
					flag = true;
				}
				append(sb, value);
			}
			sb.append("]");
			return sb.toString();
		}
		return null;
	}

	/**
	 * 将参数集转化为JSON字符串
	 * 
	 * @param params
	 *            参数集
	 * @return 返回参数集的JSON字符串
	 */
	private static final String toJSONString(Object... params) {
		if (params != null) {
			StringBuilder sb = new StringBuilder("[");
			for (int i = 0; i < params.length; i++) {
				Object value = params[i];
				if (i > 0) {
					sb.append(", ");
				}
				append(sb, value);
			}
			sb.append("]");
			return sb.toString();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private static final void append(StringBuilder sb, Object value) {
		if (value == null) {
			sb.append("null");
		} else {
			if (value instanceof String) {
				appendString(sb, (String) value);
			} else if (value instanceof Number || value instanceof Date || value instanceof Calendar
					|| value instanceof Boolean || value instanceof BigDecimal) {
				sb.append(value.toString());
			} else if (value instanceof Collection) {
				sb.append(toJSONString((Collection<Object>) value));
			} else if (value instanceof Object[]) {
				sb.append(toJSONString((Object[]) value));
			} else {
				appendString(sb, value.toString());
			}
		}
	}

	private static final void appendString(StringBuilder sb, String s) {
		sb.append("\"").append(s).append("\"");
	}
}
