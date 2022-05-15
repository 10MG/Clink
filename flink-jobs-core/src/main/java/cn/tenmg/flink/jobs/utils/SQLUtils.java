package cn.tenmg.flink.jobs.utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.parser.FlinkSQLParamsParser;

/**
 * SQL工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public abstract class SQLUtils {

	public static final String SINGLE_QUOTATION_MARK = "'", SPACE_EQUALS_SPACE = " = ", TABLE_NAME = "table-name",
			RESERVED_KEYWORD_WRAP_PREFIX = "`", RESERVED_KEYWORD_WRAP_SUFFIX = "`";

	private static final Pattern passwordPattern = Pattern.compile("\\'password\\'[\\s]*\\=[\\s]*\\'[^']*[^,]*\\'"),
			WITH_CLAUSE_PATTERN = Pattern.compile("[W|w][I|i][T|t][H|h][\\s]*\\([\\s\\S]*\\)[\\s]*$"),
			CREATE_CLAUSE_PATTERN = Pattern
					.compile("[C|c][R|r][E|e][A|a][T|t][E|e][\\s]+[T|t][A|a][B|b][L|l][E|e][\\s]+[^\\s\\(]+");

	private static final Set<String> sqlReservedKeywords = new HashSet<String>(),
			smartTableMameConnectors = new HashSet<String>();

	static {
		addReservedKeywords(FlinkJobsContext.getProperty("flink.sql.reserved.keywords"));
		addReservedKeywords(FlinkJobsContext.getProperty("flink.sql.custom.keywords"));
		String config = FlinkJobsContext.getProperty("flink.sql.smart.table-name");
		if (config != null) {
			String[] connectors = config.split(",");
			for (int i = 0; i < connectors.length; i++) {
				smartTableMameConnectors.add(connectors[i].trim());
			}
		}
		addReservedKeywords(FlinkJobsContext.getProperty("sql.reserved.keywords"));// 废弃，暂时保留兼容
		addReservedKeywords(FlinkJobsContext.getProperty("sql.custom.keywords"));// 废弃，暂时保留兼容

	}

	/**
	 * 将使用命名参数的脚本对象模型转换为可运行的Flink SQL
	 * 
	 * @param namedScript 使用命名参数的脚本对象模型
	 * @return 返回可运行的Flink SQL
	 */
	public static String toSQL(NamedScript namedScript) {
		return toSQL(namedScript.getScript(), namedScript.getParams());
	}

	/**
	 * 根据参数查找表将使用命名参数的脚本转换为可运行的Flink SQL
	 * 
	 * @param namedscript 使用命名参数的脚本
	 * @param params      参数查找表
	 * @return 返回可运行的Flink SQL
	 */
	public static String toSQL(String namedscript, Map<String, ?> params) {
		return DSLUtils.toScript(namedscript, params, FlinkSQLParamsParser.getInstance()).getValue();
	}

	/**
	 * 向SQL追加数据源配置
	 * 
	 * @param sqlBuffer  SQL缓冲器
	 * @param dataSource 数据源配置查找表
	 */
	public static void appendDataSource(StringBuffer sqlBuffer, Map<String, String> dataSource) {
		Iterator<Entry<String, String>> it = dataSource.entrySet().iterator();
		Entry<String, String> entry = it.next();
		appendProperty(sqlBuffer, entry);
		while (it.hasNext()) {
			entry = it.next();
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE);
			appendProperty(sqlBuffer, entry);
		}
	}

	/**
	 * 包装SQL字符串
	 * 
	 * @param value 字符串
	 * @return 返回包装后的SQL字符串
	 */
	public static String wrapString(String value) {
		return SINGLE_QUOTATION_MARK + value.replaceAll(SINGLE_QUOTATION_MARK, "\\\\'") + SINGLE_QUOTATION_MARK;
	}

	/**
	 * 追加空格等号空格
	 * 
	 * @param sqlBuffer SQL缓冲器
	 */
	public static void apppendEquals(StringBuffer sqlBuffer) {
		sqlBuffer.append(SPACE_EQUALS_SPACE);
	}

	/**
	 * 隐藏密码
	 * 
	 * @param sql SQL
	 * @return 隐藏密码的SQL
	 */
	public static String hiddePassword(String sql) {
		Matcher matcher = passwordPattern.matcher(sql);
		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			matcher.appendReplacement(sb, "'password' = <hidden>");
		}
		matcher.appendTail(sb);
		return sb.toString();
	}

	/**
	 * 包装数据源，即包装Flink SQL的CREATE TABLE语句的WITH子句
	 * 
	 * @param script SQL脚本
	 * @throws IOException I/O异常
	 */
	public static String wrapDataSource(String script, Map<String, String> dataSource) throws IOException {
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
				if (needDefaultTableName(dataSource) && !dataSource.containsKey(TABLE_NAME)) {
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
				if (needDefaultTableName(actualDataSource) && !config.containsKey(TABLE_NAME)
						&& !actualDataSource.containsKey(TABLE_NAME)) {
					apppendDefaultTableName(sqlBuffer, script);
				}
				sqlBuffer.append(blank.reverse()).append(end);
			}
		} else {
			sqlBuffer.append(script);
			sqlBuffer.append(" WITH (");
			SQLUtils.appendDataSource(sqlBuffer, dataSource);
			if (needDefaultTableName(dataSource) && !dataSource.containsKey(TABLE_NAME)) {
				apppendDefaultTableName(sqlBuffer, script);
			}
			sqlBuffer.append(")");
		}
		return sqlBuffer.toString();
	}

	public static String wrapIfReservedKeywords(String word) {
		if (word != null && sqlReservedKeywords.contains(word.toUpperCase())) {
			return RESERVED_KEYWORD_WRAP_PREFIX + word + RESERVED_KEYWORD_WRAP_SUFFIX;
		}
		return word;
	}

	/**
	 * 判断一个数据源是否需要添加默认的table-name
	 * 
	 * @param dataSource 数据源
	 * @return 如果数据源需要添加默认的table-name则返回true，否则返回false
	 */
	private static boolean needDefaultTableName(Map<String, String> dataSource) {
		String connector, config;
		for (Iterator<String> it = smartTableMameConnectors.iterator(); it.hasNext();) {
			connector = it.next();
			config = dataSource.get("connector");
			if (connector.equals(config)) {
				return true;
			} else if (config != null) {
				if (connector.endsWith("*")) {
					if (config.startsWith(connector.substring(0, connector.length() - 1))) {
						return true;
					}
				} else if (connector.startsWith("*")) {
					if (config.endsWith(connector.substring(1, connector.length()))) {
						return true;
					}
				}
			}
		}
		return false;
	}

	/**
	 * 追加默认表名，默认表名从CREATE语句中获取
	 * 
	 * @param sqlBuffer SQL语句缓冲器
	 * @param script    原SQL脚本
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

	private static void appendProperty(StringBuffer sqlBuffer, Entry<String, String> entry) {
		sqlBuffer.append(wrapKey(entry.getKey()));
		apppendEquals(sqlBuffer);
		sqlBuffer.append(wrapValue(entry.getValue()));
	}

	private static String wrapKey(String value) {
		return isString(value) ? value : wrapString(value);
	}

	private static boolean isString(String value) {
		return value.startsWith(SINGLE_QUOTATION_MARK) && value.endsWith(SINGLE_QUOTATION_MARK);
	}

	/**
	 * 包装配置的值
	 * 
	 * @param value 配置的值
	 * @return 返回包装后的配置值
	 */
	private static String wrapValue(String value) {
		if (value == null) {
			return "null";
		} else if (isString(value)) {
			return value;
		}
		return wrapString(value);
	}

	private static void addReservedKeywords(String keywords) {
		if (StringUtils.isNotBlank(keywords)) {
			String[] words = keywords.split(",");
			for (int i = 0; i < words.length; i++) {
				sqlReservedKeywords.add(words[i].trim().toUpperCase());
			}
		}
	}

}
