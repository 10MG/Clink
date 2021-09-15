package cn.tenmg.flink.jobs.utils;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * SQL工具类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public abstract class SQLUtils {

	public static final String SINGLE_QUOTATION_MARK = "'";

	private static final String SPACE_EQUALS_SPACE = " = ", /* DURATIONS[] = { "d", "h", "m", "s", "ms" }, */
			DATE_PATTERN = "yyyy-MM-dd HH:mm:ss", TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.S",
			TIME_PATTERN = "HH:mm:ss";

	@Deprecated
	public static final String COMMA_SPACE = ", ";

	/**
	 * 向SQL追加数据源配置
	 * 
	 * @param sqlBuffer
	 *            SQL缓冲器
	 * @param dataSource
	 *            数据源配置查找表
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

	/*
	 * private static boolean isDuration(String value) { for (int i = 0; i <
	 * DURATIONS.length; i++) { if (value.endsWith(DURATIONS[i]) &&
	 * StringUtils.isNumber(value.substring(0, value.length() -
	 * DURATIONS[i].length()))) { return true; } } return false; }
	 */

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

	/**
	 * 将指定的含命名参数的脚本转换为JDBC可执行的SQL对象，该对象内含SQL脚本及对应的参数列表。请使用cn.tenmg.dsl.utils.DSLUtils.toScript(namedscript.getScript(),
	 * namedscript.getParams(),cn.tenmg.dsl.utils.JDBCParamsParser.getInstance())替换
	 * 
	 * @param namedScript
	 *            含命名参数的脚本
	 * @return 返回JDBC可执行的SQL对象，含SQL脚本及对应的参数列表
	 */
	@Deprecated
	public static JDBC toJDBC(NamedScript namedScript) {
		List<Object> paramList = new ArrayList<Object>();
		String script = namedScript.getScript();
		if (StringUtils.isBlank(script)) {
			return new JDBC(script, paramList);
		}
		Map<String, Object> params = namedScript.getParams();
		if (params == null) {
			params = new HashMap<String, Object>();
		}
		int len = script.length(), i = 0, backslashes = 0;
		char a = DSLUtils.BLANK_SPACE, b = DSLUtils.BLANK_SPACE;
		boolean isString = false;// 是否在字符串区域
		boolean isParam = false;// 是否在参数区域
		StringBuilder sql = new StringBuilder(), paramName = new StringBuilder();
		while (i < len) {
			char c = script.charAt(i);
			if (isString) {
				if (c == DSLUtils.BACKSLASH) {
					backslashes++;
				} else {
					if (DSLUtils.isStringEnd(a, b, c, backslashes)) {// 字符串区域结束
						isString = false;
					}
					backslashes = 0;
				}
				sql.append(c);
			} else {
				if (c == DSLUtils.SINGLE_QUOTATION_MARK) {// 字符串区域开始
					isString = true;
					sql.append(c);
				} else if (isParam) {// 处于参数区域
					if (DSLUtils.isParamChar(c)) {
						paramName.append(c);
					} else {
						isParam = false;// 参数区域结束
						paramEnd(params, sql, paramName, paramList);
						sql.append(c);
					}
				} else {
					if (DSLUtils.isParamBegin(a, b, c)) {
						isParam = true;// 参数区域开始
						paramName.setLength(0);
						paramName.append(c);
						sql.setCharAt(sql.length() - 1, '?');// “:”替换为“?”
					} else {
						sql.append(c);
					}
				}
			}
			a = b;
			b = c;
			i++;
		}
		if (isParam) {
			paramEnd(params, sql, paramName, paramList);
		}
		return new JDBC(sql.toString(), paramList);
	}

	/**
	 * 将指定的含命名参数的脚本转换为Flink
	 * SQL。请使用cn.tenmg.dsl.utils.DSLUtils.toScript(namedscript.getScript(),
	 * namedscript.getParams(),
	 * cn.tenmg.flink.jobs.parser.FlinkSQLParamsParser.getInstance()).getValue()替换
	 * 
	 * @param namedScript
	 *            含命名参数的脚本
	 * @return 返回Flink SQL
	 */
	@Deprecated
	public static String toSQL(NamedScript namedScript) {
		String source = namedScript.getScript();
		if (StringUtils.isBlank(source)) {
			return source;
		}
		Map<String, Object> params = namedScript.getParams();
		if (params == null) {
			params = new HashMap<String, Object>();
		}
		int len = source.length(), i = 0, backslashes = 0;
		char a = DSLUtils.BLANK_SPACE, b = DSLUtils.BLANK_SPACE;
		boolean isString = false;// 是否在字符串区域
		boolean isParam = false;// 是否在参数区域
		StringBuilder sqlBuilder = new StringBuilder(), paramName = new StringBuilder();
		while (i < len) {
			char c = source.charAt(i);
			if (isString) {
				if (c == DSLUtils.BACKSLASH) {
					backslashes++;
				} else {
					if (DSLUtils.isStringEnd(a, b, c, backslashes)) {// 字符串区域结束
						isString = false;
					}
					backslashes = 0;
				}
				sqlBuilder.append(c);
			} else {
				if (c == DSLUtils.SINGLE_QUOTATION_MARK) {// 字符串区域开始
					isString = true;
					sqlBuilder.append(c);
				} else if (isParam) {// 处于参数区域
					if (DSLUtils.isParamChar(c)) {
						paramName.append(c);
					} else {
						isParam = false;// 参数区域结束
						String name = paramName.toString();
						parseParam(sqlBuilder, name, params.get(name));
						sqlBuilder.append(c);
					}
				} else {
					if (DSLUtils.isParamBegin(a, b, c)) {
						isParam = true;// 参数区域开始
						paramName.setLength(0);
						paramName.append(c);
						sqlBuilder.setLength(sqlBuilder.length() - 1);// 去掉 “:”
					} else {
						sqlBuilder.append(c);
					}
				}
			}
			a = b;
			b = c;
			i++;
		}
		if (isParam) {
			String name = paramName.toString();
			parseParam(sqlBuilder, name, params.get(name));
		}
		return sqlBuilder.toString();
	}

	@Deprecated
	public static class JDBC {

		private String statement;

		private List<Object> params;

		public String getStatement() {
			return statement;
		}

		public void setStatement(String statement) {
			this.statement = statement;
		}

		public List<Object> getParams() {
			return params;
		}

		public void setParams(List<Object> params) {
			this.params = params;
		}

		public JDBC() {
			super();
		}

		public JDBC(String statement, List<Object> params) {
			super();
			this.statement = statement;
			this.params = params;
		}
	}

	private static void parseParam(StringBuilder sb, String name, Object value) {
		if (value == null) {
			appendNull(sb);
		} else {
			if (value instanceof Collection<?>) {
				Collection<?> collection = (Collection<?>) value;
				if (collection == null || collection.isEmpty()) {
					appendNull(sb);
				} else {
					boolean flag = false;
					for (Iterator<?> it = collection.iterator(); it.hasNext();) {
						if (flag) {
							sb.append(COMMA_SPACE);
						} else {
							flag = true;
						}
						append(sb, it.next());
					}
				}
			} else if (value instanceof Object[]) {
				Object[] objects = (Object[]) value;
				if (objects.length == 0) {
					appendNull(sb);
				} else {
					for (int j = 0; j < objects.length; j++) {
						if (j > 0) {
							sb.append(COMMA_SPACE);
						}
						append(sb, objects[j]);
					}
				}
			} else {
				append(sb, value);
			}
		}
	}

	private static void append(StringBuilder sqlBuilder, Object value) {
		if (value instanceof String || value instanceof char[]) {
			appendString(sqlBuilder, (String) value);
		} else if (value instanceof Date) {
			appendDate(sqlBuilder, (Date) value);
		} else if (value instanceof Calendar) {
			Date date = ((Calendar) value).getTime();
			if (date == null) {
				appendNull(sqlBuilder);
			} else {
				appendDate(sqlBuilder, date);
			}
		} else {
			sqlBuilder.append(value.toString());
		}
	}

	private static final void appendNull(StringBuilder sqlBuilder) {
		sqlBuilder.append("NULL");
	}

	private static final void appendString(StringBuilder sqlBuilder, String value) {
		sqlBuilder.append("'").append(value).append("'");
	}

	private static final void appendDate(StringBuilder sqlBuilder, Date value) {
		String s;
		if (value instanceof Timestamp) {
			s = "TO_TIMESTAMP('" + DateUtils.format(value, TIMESTAMP_PATTERN) + "', '" + TIMESTAMP_PATTERN + "')";
		} else if (value instanceof Time) {
			s = "TIME('" + DateUtils.format(value, TIME_PATTERN) + "', '" + TIME_PATTERN + "')";
		} else {
			s = "TO_DATE('" + DateUtils.format(value, DATE_PATTERN) + "', '" + DATE_PATTERN + "')";
		}
		sqlBuilder.append(s);
	}

	private static void paramEnd(Map<String, ?> params, StringBuilder sqlBuilder, StringBuilder paramName,
			List<Object> paramList) {
		String name = paramName.toString();
		Object value = params.get(name);
		if (value != null) {
			if (value instanceof Collection<?>) {
				Collection<?> collection = (Collection<?>) value;
				if (collection == null || collection.isEmpty()) {
					paramList.add(null);
				} else {
					boolean flag = false;
					for (Iterator<?> it = collection.iterator(); it.hasNext();) {
						if (flag) {
							sqlBuilder.append(", ?");
						} else {
							flag = true;
						}
						paramList.add(it.next());
					}
				}
			} else if (value instanceof Object[]) {
				Object[] objects = (Object[]) value;
				if (objects.length == 0) {
					paramList.add(null);
				} else {
					for (int j = 0; j < objects.length; j++) {
						if (j > 0) {
							sqlBuilder.append(", ?");
						}
						paramList.add(objects[j]);
					}
				}
			} else {
				paramList.add(value);
			}
		} else {
			paramList.add(value);
		}
	}

}
