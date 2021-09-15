package cn.tenmg.flink.jobs.operator;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.Script;
import cn.tenmg.dsl.parser.JDBCParamsParser;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.model.ExecuteSql;
import cn.tenmg.flink.jobs.parser.FlinkSQLParamsParser;
import cn.tenmg.flink.jobs.utils.ConfigurationUtils;
import cn.tenmg.flink.jobs.utils.JDBCUtils;
import cn.tenmg.flink.jobs.utils.JSONUtils;
import cn.tenmg.flink.jobs.utils.MapUtils;
import cn.tenmg.flink.jobs.utils.SQLUtils;

/**
 * SQL操作执行器
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 * @since 1.1.0
 */
public class ExecuteSqlOperator extends AbstractSqlOperator<ExecuteSql> {

	private static final Logger log = LogManager.getLogger(ExecuteSqlOperator.class);

	private static final String TABLE_NAME = "table-name",
			/**
			 * 删除语句正则表达式
			 */
			DELETE_CLAUSE_REGEX = "[\\s]*[D|d][E|e][L|l][E|e][T|t][E|e][\\s]+[F|f][R|r][O|o][M|m][\\s]+[\\S]+",

			/**
			 * 更新语句正则表达式
			 */
			UPDATE_CLAUSE_REGEX = "[\\s]*[U|u][P|p][D|d][A|a][T|t][E|e][\\s]+[\\S]+[\\s]+[S|s][E|e][T|t][\\s]+[\\S]+";

	private static final Pattern WITH_CLAUSE_PATTERN = Pattern
			.compile("[W|w][I|i][T|t][H|h][\\s]*\\([\\s\\S]*\\)[\\s]*$"),
			CREATE_CLAUSE_PATTERN = Pattern
					.compile("[C|c][R|r][E|e][A|a][T|t][E|e][\\s]+[T|t][A|a][B|b][L|l][E|e][\\s]+[\\S]+");

	@Override
	Object execute(StreamTableEnvironment tableEnv, ExecuteSql sql, Map<String, Object> params) throws Exception {
		NamedScript namedScript = DSLUtils.parse(sql.getScript(), params);
		String datasource = sql.getDataSource(), statement = namedScript.getScript();
		if (StringUtils.isNotBlank(datasource)) {
			Map<String, String> dataSource = FlinkJobsContext.getDatasource(datasource);
			if (ConfigurationUtils.isJDBC(dataSource)
					&& (statement.matches(DELETE_CLAUSE_REGEX) || statement.matches(UPDATE_CLAUSE_REGEX))) {// DELETE/UPDATE语句，使用JDBC执行
				Script<List<Object>> script = DSLUtils.toScript(namedScript.getScript(), namedScript.getParams(),
						JDBCParamsParser.getInstance());
				statement = script.getValue();
				Connection con = null;
				PreparedStatement ps = null;
				try {
					con = JDBCUtils.getConnection(dataSource);// 获得数据库连接
					con.setAutoCommit(true);
					ps = con.prepareStatement(statement);
					List<Object> parameters = script.getParams();
					JDBCUtils.setParams(ps, parameters);
					if (log.isInfoEnabled()) {
						log.info("SQL: " + statement + ", parameters: " + JSONUtils.toJSONString(parameters));
					}
					return ps.executeLargeUpdate();// 执行删除
				} catch (Exception e) {
					throw e;
				} finally {
					JDBCUtils.close(ps);
					JDBCUtils.close(con);
				}
			} else {
				statement = DSLUtils
						.toScript(namedScript.getScript(), namedScript.getParams(), FlinkSQLParamsParser.getInstance())
						.getValue();
			}
			statement = wrapDataSource(statement, dataSource);
		}
		if (log.isInfoEnabled()) {
			log.info(statement);
		}
		return tableEnv.executeSql(statement);
	}

	/**
	 * 包装数据源，即包装Flink SQL的CREATE TABLE语句的WITH子句
	 * 
	 * @param script
	 *            SQL脚本
	 * @throws IOException
	 *             I/O异常
	 */
	private static String wrapDataSource(String script, Map<String, String> dataSource) throws IOException {
		Matcher matcher = WITH_CLAUSE_PATTERN.matcher(script);
		StringBuffer sqlBuffer = new StringBuffer();
		if (matcher.find()) {
			String group = matcher.group();
			int startIndex = group.indexOf("(") + 1, endIndex = group.lastIndexOf(")");
			String start = group.substring(0, startIndex), value = group.substring(startIndex, endIndex),
					end = group.substring(endIndex);
			if (StringUtils.isBlank(value)) {
				matcher.appendReplacement(sqlBuffer, start);
				SQLUtils.appendDataSource(sqlBuffer, dataSource);
				if (!dataSource.containsKey(TABLE_NAME)) {
					apppendDefaultTableName(sqlBuffer, script);
				}
				sqlBuffer.append(end);
			} else {
				Map<String, String> config = ConfigurationUtils.load(value),
						actualDataSource = MapUtils.newHashMap(dataSource);
				MapUtils.removeAll(actualDataSource, config.keySet());
				matcher.appendReplacement(sqlBuffer, start);
				StringBuilder blank = new StringBuilder();
				int len = value.length(), i = len - 1;
				while (i > 0) {
					char c = value.charAt(i);
					if (c > DSLUtils.BLANK_SPACE) {
						break;
					}
					blank.append(c);
					i--;
				}
				sqlBuffer.append(value.substring(0, i + 1)).append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE);
				SQLUtils.appendDataSource(sqlBuffer, actualDataSource);
				if (ConfigurationUtils.isJDBC(actualDataSource) && !config.containsKey(TABLE_NAME)
						&& !actualDataSource.containsKey(TABLE_NAME)) {
					apppendDefaultTableName(sqlBuffer, script);
				}
				sqlBuffer.append(blank.reverse()).append(end);
			}
		} else {
			sqlBuffer.append(script);
			sqlBuffer.append(" WITH (");
			SQLUtils.appendDataSource(sqlBuffer, dataSource);
			if (!dataSource.containsKey(TABLE_NAME)) {
				apppendDefaultTableName(sqlBuffer, script);
			}
			sqlBuffer.append(")");
		}
		return sqlBuffer.toString();
	}

	/**
	 * 追加默认表名，默认表名从CREATE语句中获取
	 * 
	 * @param sqlBuffer
	 *            SQL语句缓冲器
	 * @param script
	 *            原SQL脚本
	 */
	private static void apppendDefaultTableName(StringBuffer sqlBuffer, String script) {
		Matcher createMatcher = CREATE_CLAUSE_PATTERN.matcher(script);
		if (createMatcher.find()) {
			String group = createMatcher.group();
			StringBuilder tableNameBuilder = new StringBuilder();
			int i = group.length();
			while (--i > 0) {
				char c = group.charAt(i);
				if (c > DSLUtils.BLANK_SPACE) {
					tableNameBuilder.append(c);
					break;
				}
			}
			while (--i > 0) {
				char c = group.charAt(i);
				if (c > DSLUtils.BLANK_SPACE) {
					tableNameBuilder.append(c);
				} else {
					break;
				}
			}
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(SQLUtils.wrapString(TABLE_NAME));
			SQLUtils.apppendEquals(sqlBuffer);
			sqlBuffer.append(SQLUtils.wrapString(tableNameBuilder.reverse().toString()));
		}
	}

}
