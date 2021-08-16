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

import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.utils.NamedScriptUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * SQL工具类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public abstract class SQLUtils {

	private static final char SINGLE_QUOTATION_MARK = '\'';

	private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss", TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.S",
			TIME_PATTERN = "HH:mm:ss";

	public static final String COMMA_SPACE = ", ";

	/**
	 * 将指定的含命名参数的脚本转换为JDBC可执行的SQL对象，该对象内含SQL脚本及对应的参数列表
	 * 
	 * @param namedScript
	 *            含命名参数的脚本
	 * @return 返回JDBC可执行的SQL对象，含SQL脚本及对应的参数列表
	 */
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
		char a = NamedScriptUtils.BLANK_SPACE, b = NamedScriptUtils.BLANK_SPACE;
		boolean isString = false;// 是否在字符串区域
		boolean isParam = false;// 是否在参数区域
		StringBuilder sql = new StringBuilder(), paramName = new StringBuilder();
		while (i < len) {
			char c = script.charAt(i);
			if (isString) {
				if (c == NamedScriptUtils.BACKSLASH) {
					backslashes++;
				} else {
					if (NamedScriptUtils.isStringEnd(a, b, c, backslashes)) {// 字符串区域结束
						isString = false;
					}
					backslashes = 0;
				}
				sql.append(c);
			} else {
				if (c == SINGLE_QUOTATION_MARK) {// 字符串区域开始
					isString = true;
					sql.append(c);
				} else if (isParam) {// 处于参数区域
					if (NamedScriptUtils.isParamChar(c)) {
						paramName.append(c);
					} else {
						isParam = false;// 参数区域结束
						paramEnd(params, sql, paramName, paramList);
						sql.append(c);
					}
				} else {
					if (NamedScriptUtils.isParamBegin(a, b, c)) {
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
	 * 将指定的含命名参数的脚本转换为Flink SQL
	 * 
	 * @param namedScript
	 *            含命名参数的脚本
	 * @return 返回Flink SQL
	 */
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
		char a = NamedScriptUtils.BLANK_SPACE, b = NamedScriptUtils.BLANK_SPACE;
		boolean isString = false;// 是否在字符串区域
		boolean isParam = false;// 是否在参数区域
		StringBuilder sqlBuilder = new StringBuilder(), paramName = new StringBuilder();
		while (i < len) {
			char c = source.charAt(i);
			if (isString) {
				if (c == NamedScriptUtils.BACKSLASH) {
					backslashes++;
				} else {
					if (NamedScriptUtils.isStringEnd(a, b, c, backslashes)) {// 字符串区域结束
						isString = false;
					}
					backslashes = 0;
				}
				sqlBuilder.append(c);
			} else {
				if (c == SINGLE_QUOTATION_MARK) {// 字符串区域开始
					isString = true;
					sqlBuilder.append(c);
				} else if (isParam) {// 处于参数区域
					if (NamedScriptUtils.isParamChar(c)) {
						paramName.append(c);
					} else {
						isParam = false;// 参数区域结束
						String name = paramName.toString();
						parseParam(sqlBuilder, name, params.get(name));
						sqlBuilder.append(c);
					}
				} else {
					if (NamedScriptUtils.isParamBegin(a, b, c)) {
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
