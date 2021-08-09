package cn.tenmg.flink.jobs.operator;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.model.ExecuteSql;
import cn.tenmg.flink.jobs.utils.JdbcUtils;
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

	private static final String SINGLE_QUOTATION_MARK = "'", DURATIONS[] = { "d", "h", "m", "s", "ms" },
			TABLE_NAME = "table-name",
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
		String dataSource = sql.getDataSource(), statement = SQLUtils.toSQL(namedScript);
		if (StringUtils.isNotBlank(dataSource)) {
			Map<String, String> dsConfig = FlinkJobsContext.getDatasource(dataSource);
			if (isJDBC(dsConfig)
					&& (statement.matches(DELETE_CLAUSE_REGEX) || statement.matches(UPDATE_CLAUSE_REGEX))) {// DELETE/UPDATE语句，使用JDBC执行
				Connection con = null;
				PreparedStatement stmt = null;
				try {
					JdbcUtils.loadDriver(dsConfig);
					con = DriverManager.getConnection(dsConfig.get("url"), dsConfig.get("username"),
							dsConfig.get("password"));// 获得数据库连接
					con.setAutoCommit(true);
					stmt = con.prepareStatement(statement);
					log.info(statement);
					return stmt.executeLargeUpdate();// 执行删除
				} catch (Exception e) {
					throw e;
				} finally {
					JdbcUtils.close(stmt);
					JdbcUtils.close(con);
				}
			}
			statement = wrapDataSource(statement, dsConfig);
		}
		log.info(statement);
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
				appendDataSource(sqlBuffer, dataSource);
				if (!dataSource.containsKey(TABLE_NAME)) {
					apppendDefaultTableName(sqlBuffer, script);
				}
				sqlBuffer.append(end);
			} else {
				Properties properties = new Properties();
				properties.load(new StringReader(value.replaceAll(SINGLE_QUOTATION_MARK, "")));
				LinkedHashMap<String, String> actualDataSource = new LinkedHashMap<String, String>();
				actualDataSource.putAll(dataSource);
				for (Iterator<Object> it = properties.keySet().iterator(); it.hasNext();) {
					actualDataSource.remove(it.next().toString());
				}
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
				sqlBuffer.append(value.substring(0, i + 1)).append(SQLUtils.COMMA_SPACE);
				appendDataSource(sqlBuffer, actualDataSource);
				if (isJDBC(actualDataSource) && !properties.containsKey(TABLE_NAME)
						&& !actualDataSource.containsKey(TABLE_NAME)) {
					apppendDefaultTableName(sqlBuffer, script);
				}
				sqlBuffer.append(blank.reverse()).append(end);
			}
		} else {
			sqlBuffer.append(script);
			sqlBuffer.append(" WITH (");
			appendDataSource(sqlBuffer, dataSource);
			if (!dataSource.containsKey(TABLE_NAME)) {
				apppendDefaultTableName(sqlBuffer, script);
			}
			sqlBuffer.append(")");
		}
		return sqlBuffer.toString();
	}

	private static boolean isJDBC(Map<String, String> dataSource) {
		return "jdbc".equals(dataSource.get("connector"));
	}

	/**
	 * 向SQL追加数据源配置
	 * 
	 * @param sqlBuffer
	 *            SQL缓冲器
	 * @param dataSource
	 *            数据源配置查找表
	 */
	private static void appendDataSource(StringBuffer sqlBuffer, Map<String, String> dataSource) {
		boolean flag = false;
		for (Iterator<Entry<String, String>> it = dataSource.entrySet().iterator(); it.hasNext();) {
			Entry<String, String> entry = it.next();
			if (flag) {
				sqlBuffer.append(SQLUtils.COMMA_SPACE);
			} else {
				flag = true;
			}
			sqlBuffer.append(wrapKey(entry.getKey()));
			apppendEquals(sqlBuffer);
			sqlBuffer.append(wrapValue(entry.getValue()));
		}
	}

	private static String wrapKey(String value) {
		return isString(value) ? value : wrapString(value);
	}

	/**
	 * 包装配置的值
	 * 
	 * @param value
	 *            配置的值
	 * @return 返回包装后的配置值
	 */
	private static String wrapValue(String value) {
		if (StringUtils.isBlank(value)) {
			return SINGLE_QUOTATION_MARK + value + SINGLE_QUOTATION_MARK;
		} else if (isString(value) || StringUtils.isNumber(value) || isDuration(value)) {
			return value;
		}
		return wrapString(value);
	}

	private static String wrapString(String value) {
		return SINGLE_QUOTATION_MARK + value.replaceAll(SINGLE_QUOTATION_MARK, "\\'") + SINGLE_QUOTATION_MARK;
	}

	private static boolean isString(String value) {
		return value.startsWith(SINGLE_QUOTATION_MARK) && value.endsWith(SINGLE_QUOTATION_MARK);
	}

	private static boolean isDuration(String value) {
		for (int i = 0; i < DURATIONS.length; i++) {
			if (value.endsWith(DURATIONS[i])
					&& StringUtils.isNumber(value.substring(0, value.length() - DURATIONS[i].length()))) {
				return true;
			}
		}
		return false;
	}

	private static void apppendEquals(StringBuffer sqlBuffer) {
		sqlBuffer.append(" = ");
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
			sqlBuffer.append(SQLUtils.COMMA_SPACE).append(wrapString(TABLE_NAME));
			apppendEquals(sqlBuffer);
			sqlBuffer.append(wrapString(tableNameBuilder.reverse().toString()));
		}
	}

}
