package cn.tenmg.clink.utils;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.clink.metadata.MetaDataGetter.TableMetaData.ColumnType;
import cn.tenmg.clink.parser.FlinkSQLParamsParser;
import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.MatchUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * SQL工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public abstract class SQLUtils {

	public static final String SINGLE_QUOTATION_MARK = "'", SPACE_EQUALS_SPACE = " = ", TABLE_NAME = "table-name",
			RESERVED_KEYWORD_WRAP_PREFIX = "`", RESERVED_KEYWORD_WRAP_SUFFIX = "`", LEFT_BRACKET = "(",
			RIGTH_BRACKET = ")";

	private static final Pattern passwordPattern = Pattern.compile("\\'password\\'[\\s]*\\=[\\s]*\\'[^']*[^,]*\\'"),
			WITH_CLAUSE_PATTERN = Pattern.compile("[Ww][Ii][Tt][Hh][\\s]*\\([\\s\\S]*\\)[\\s]*$"),
			CREATE_CLAUSE_PATTERN = Pattern
					.compile("[Cc][Rr][Ee][Aa][Tt][Ee][\\s]+[Tt][Aa][Bb][Ll][Ee][\\s]+[^\\s\\(]+");

	private static final Set<String> sqlReservedKeywords = new HashSet<String>(),
			smartTableNameConnectors = new HashSet<String>();

	static {
		addReservedKeywords(ClinkContext.getProperty("flink.sql.reserved.keywords"));
		addReservedKeywords(ClinkContext.getProperty("flink.sql.custom.keywords"));
		String config = ClinkContext.getProperty("flink.sql.smart.table-name");
		if (config != null) {
			String[] connectors = config.split(",");
			for (int i = 0; i < connectors.length; i++) {
				smartTableNameConnectors.add(connectors[i].trim());
			}
		}
	}

	/**
	 * 将使用命名参数的脚本对象模型转换为可运行的Flink SQL
	 * 
	 * @param namedScript
	 *            使用命名参数的脚本对象模型
	 * @return 返回可运行的Flink SQL
	 */
	public static String toSQL(NamedScript namedScript) {
		return toSQL(namedScript.getScript(), namedScript.getParams());
	}

	/**
	 * 根据参数查找表将使用命名参数的脚本转换为可运行的Flink SQL
	 * 
	 * @param namedscript
	 *            使用命名参数的脚本
	 * @param params
	 *            参数查找表
	 * @return 返回可运行的Flink SQL
	 */
	public static String toSQL(String namedscript, Map<String, ?> params) {
		return DSLUtils.toScript(namedscript, params, FlinkSQLParamsParser.getInstance()).getValue();
	}

	/**
	 * 向SQL追加数据源配置
	 * 
	 * @param dataSource
	 *            数据源配置查找表
	 * @param sqlBuffer
	 *            SQL缓冲器
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
	 * 向SQL追加数据源配置
	 * 
	 * @param sqlBuffer
	 *            SQL缓冲器
	 * @param dataSource
	 *            数据源配置查找表
	 * @param defaultTableName
	 *            默认表名
	 */
	public static void appendDataSource(StringBuffer sqlBuffer, Map<String, String> dataSource,
			String defaultTableName) {
		appendDataSource(sqlBuffer, dataSource);
		if (needDefaultTableName(dataSource) && !dataSource.containsKey(TABLE_NAME)) {
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(wrapString(TABLE_NAME));
			apppendEquals(sqlBuffer);
			sqlBuffer.append(wrapString(defaultTableName));
		}
	}

	/**
	 * 向SQL追加数据源配置
	 * 
	 * @param script
	 *            SQL脚本
	 * @param dataSource
	 *            数据源配置查找表
	 * @param sqlBuffer
	 *            SQL缓冲器
	 */
	private static void appendDataSource(String script, Map<String, String> dataSource, StringBuffer sqlBuffer) {
		appendDataSource(sqlBuffer, dataSource);
		if (needDefaultTableName(dataSource) && !dataSource.containsKey(TABLE_NAME)) {
			extractAndApppendDefaultTableName(sqlBuffer, script);
		}
	}

	/**
	 * 包装SQL字符串
	 * 
	 * @param value
	 *            字符串
	 * @return 返回包装后的SQL字符串
	 */
	public static String wrapString(String value) {
		return SINGLE_QUOTATION_MARK + value.replaceAll(SINGLE_QUOTATION_MARK, "\\\\'") + SINGLE_QUOTATION_MARK;
	}

	/**
	 * 追加空格等号空格
	 * 
	 * @param sqlBuffer
	 *            SQL缓冲器
	 */
	public static void apppendEquals(StringBuffer sqlBuffer) {
		sqlBuffer.append(SPACE_EQUALS_SPACE);
	}

	/**
	 * 隐藏密码
	 * 
	 * @param sql
	 *            SQL
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
	 * @param script
	 *            SQL脚本
	 * @throws IOException
	 *             I/O异常
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
				SQLUtils.appendDataSource(script, dataSource, sqlBuffer);
				sqlBuffer.append(end);
			} else {
				Map<String, String> config = ConfigurationUtils.load(value),
						actualDataSource = MapUtils.toHashMap(dataSource);
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
				SQLUtils.appendDataSource(script, actualDataSource, sqlBuffer);
				sqlBuffer.append(blank.reverse()).append(end);
			}
		} else {
			sqlBuffer.append(script);
			sqlBuffer.append(" WITH (");
			SQLUtils.appendDataSource(script, dataSource, sqlBuffer);
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
	 * @param dataSource
	 *            数据源
	 * @return 如果数据源需要添加默认的table-name则返回true，否则返回false
	 */
	public static boolean needDefaultTableName(Map<String, String> dataSource) {
		return MatchUtils.matchesAny(smartTableNameConnectors, dataSource.get("connector"));
	}

	/**
	 * 向SQL语句缓冲器中追加默认表名
	 * 
	 * @param sqlBuffer
	 *            SQL语句缓冲器
	 * @param defaultTableName
	 *            默认表名
	 */
	public static void apppendDefaultTableName(StringBuffer sqlBuffer, String defaultTableName) {
		sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(wrapString(TABLE_NAME));
		apppendEquals(sqlBuffer);
		sqlBuffer.append(wrapString(wrapIfReservedKeywords(defaultTableName)));
	}

	/**
	 * 转换为 Flink SQL 的数据类型
	 * 
	 * @param connector
	 *            连接器（如mysql等）
	 * @param columnType
	 *            列类型
	 * @return 对应的 Flink SQL 的数据类型
	 */
	public static String toSQLType(String connector, ColumnType columnType) {
		int scale = columnType.getScale(), precision = columnType.getPrecision();
		String typeName = columnType.getTypeName();
		String type = getPossibleType(connector, typeName, scale, precision);// 根据类型名称找对应的类型
		if (StringUtils.isBlank(type)) {
			String jdbcType = JDBC_TYPES.get(columnType.getDataType());
			type = getPossibleType(connector, jdbcType, precision, scale);// 根据 JDBC 类型枚举找对应的类型
			if (StringUtils.isBlank(type)) {
				type = getDefaultType(jdbcType, precision, scale);// 使用默认的类型
			}
		}
		if (columnType.isNotNull()) {
			return wrapType(type, precision, scale).concat(" NOT NULL");
		} else {
			return wrapType(type, precision, scale);
		}
	}

	/**
	 * 从CREATE语句中提取并追加默认表名
	 * 
	 * @param sqlBuffer
	 *            SQL语句缓冲器
	 * @param script
	 *            原SQL脚本
	 */
	private static void extractAndApppendDefaultTableName(StringBuffer sqlBuffer, String script) {
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
			apppendDefaultTableName(sqlBuffer, tableNameBuilder.reverse().toString());
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
	 * @param value
	 *            配置的值
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

	private static final String TYPE_PREFFIX = "flink.sql.type" + ClinkContext.CONFIG_SPLITER,
			DEFAULT_TYPE = ClinkContext.getProperty(TYPE_PREFFIX + "default");

	private static final Map<Integer, String> JDBC_TYPES = MapUtils
			.newHashMapBuilder(java.sql.Types.VARCHAR, "java.sql.Types.VARCHAR")
			.put(java.sql.Types.CHAR, "java.sql.Types.CHAR").put(java.sql.Types.NVARCHAR, "java.sql.Types.NVARCHAR")
			.put(java.sql.Types.NCHAR, "java.sql.Types.NCHAR")
			.put(java.sql.Types.LONGNVARCHAR, "java.sql.Types.LONGNVARCHAR")
			.put(java.sql.Types.LONGVARCHAR, "java.sql.Types.LONGVARCHAR")
			.put(java.sql.Types.BIGINT, "java.sql.Types.BIGINT").put(java.sql.Types.BOOLEAN, "java.sql.Types.BOOLEAN")
			.put(java.sql.Types.BIT, "java.sql.Types.BIT").put(java.sql.Types.DECIMAL, "java.sql.Types.DECIMAL")
			.put(java.sql.Types.OTHER, "java.sql.Types.OTHER").put(java.sql.Types.DOUBLE, "java.sql.Types.DOUBLE")
			.put(java.sql.Types.FLOAT, "java.sql.Types.FLOAT").put(java.sql.Types.REAL, "java.sql.Types.REAL")
			.put(java.sql.Types.INTEGER, "java.sql.Types.INTEGER").put(java.sql.Types.NUMERIC, "java.sql.Types.NUMERIC")
			.put(java.sql.Types.SMALLINT, "java.sql.Types.SMALLINT")
			.put(java.sql.Types.TINYINT, "java.sql.Types.TINYINT").put(java.sql.Types.DATE, "java.sql.Types.DATE")
			.put(java.sql.Types.TIME, "java.sql.Types.TIME")
			.put(java.sql.Types.TIME_WITH_TIMEZONE, "java.sql.Types.TIME_WITH_TIMEZONE")
			.put(java.sql.Types.TIMESTAMP, "java.sql.Types.TIMESTAMP")
			.put(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, "java.sql.Types.TIMESTAMP_WITH_TIMEZONE")
			.put(java.sql.Types.BINARY, "java.sql.Types.BINARY")
			.put(java.sql.Types.LONGVARBINARY, "java.sql.Types.LONGVARBINARY")
			.put(java.sql.Types.VARBINARY, "java.sql.Types.VARBINARY").put(java.sql.Types.REF, "java.sql.Types.REF")
			.put(java.sql.Types.DATALINK, "java.sql.Types.DATALINK").put(java.sql.Types.ARRAY, "java.sql.Types.ARRAY")
			.put(java.sql.Types.BLOB, "java.sql.Types.BLOB").put(java.sql.Types.CLOB, "java.sql.Types.CLOB")
			.put(java.sql.Types.NCLOB, "java.sql.Types.NCLOB").build(java.sql.Types.STRUCT, "java.sql.Types.STRUCT");

	private static Set<String> WITH_PRECISION = uppercaseSet(
			ClinkContext.getProperty(Arrays.asList("flink.sql.type.with_precision", "flink.sql.type.with-precision"))),
			WITH_SIZE = uppercaseSet(
					ClinkContext.getProperty(Arrays.asList("flink.sql.type.with_size", "flink.sql.type.with-size")));// 兼容老的配置

	private static String getPossibleType(String connector, String typeName, int scale, int precision) {
		String type = ClinkContext.getProperty(StringUtils.concat(TYPE_PREFFIX, connector, ClinkContext.CONFIG_SPLITER,
				typeName, LEFT_BRACKET + scale + "," + precision + RIGTH_BRACKET));
		if (StringUtils.isBlank(type)) {
			type = ClinkContext.getProperty(StringUtils.concat(TYPE_PREFFIX, connector, ClinkContext.CONFIG_SPLITER,
					typeName, LEFT_BRACKET, scale, RIGTH_BRACKET));
			if (StringUtils.isBlank(type)) {
				type = ClinkContext.getProperty(
						StringUtils.concat(TYPE_PREFFIX, connector, ClinkContext.CONFIG_SPLITER, typeName));
			}
		}
		return type;
	}

	private static String getDefaultType(String type, int scale, int precision) {
		if (type == null) {
			return DEFAULT_TYPE;
		}
		String possibleType = ClinkContext.getProperty(
				StringUtils.concat(TYPE_PREFFIX, type, LEFT_BRACKET, scale, "," + precision, RIGTH_BRACKET));
		if (StringUtils.isBlank(possibleType)) {
			possibleType = ClinkContext
					.getProperty(StringUtils.concat(TYPE_PREFFIX, type, LEFT_BRACKET, scale, RIGTH_BRACKET));
			if (StringUtils.isBlank(possibleType)) {
				possibleType = ClinkContext.getProperty(TYPE_PREFFIX.concat(type));
			}
		}
		if (StringUtils.isBlank(possibleType)) {
			return DEFAULT_TYPE;
		} else {
			return possibleType;
		}
	}

	private static String wrapType(String type, int precision, int scale) {
		String typeUpperCase = type.toUpperCase();
		if (WITH_PRECISION.contains(typeUpperCase)) {// 类型含精度
			return StringUtils.concat(type, LEFT_BRACKET, precision, ",", scale, RIGTH_BRACKET);
		} else if (WITH_SIZE.contains(typeUpperCase)) {// 类型含长度
			String prefix = StringUtils.concat(TYPE_PREFFIX, type, ClinkContext.CONFIG_SPLITER),
					sizeOffset = ClinkContext
							.getProperty(Arrays.asList(prefix.concat("size_offset"), prefix.concat("size-offset")));// 兼容老的配置
			if (StringUtils.isBlank(sizeOffset)) {
				return StringUtils.concat(type, LEFT_BRACKET, precision, RIGTH_BRACKET);
			} else {
				int offset = Integer.parseInt(sizeOffset);
				if (scale >= offset) {
					return StringUtils.concat(type, LEFT_BRACKET, (precision - offset), RIGTH_BRACKET);
				}
			}
		}
		return type;
	}

	private static final Set<String> uppercaseSet(String string) {
		Set<String> set = new HashSet<String>();
		if (string != null) {
			String[] strings = string.split(",");
			for (int i = 0; i < strings.length; i++) {
				set.add(strings[i].trim().toUpperCase());
			}
		}
		return set;
	}

}
