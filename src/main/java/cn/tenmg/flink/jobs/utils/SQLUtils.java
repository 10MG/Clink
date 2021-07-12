package cn.tenmg.flink.jobs.utils;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

	private static final char SINGLE_QUOTATION_MARK = '\'';

	private static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss", TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.S",
			TIME_PATTERN = "HH:mm:ss";

	public static final String COMMA_SPACE = ", ";

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
				if (c == SINGLE_QUOTATION_MARK) {// 字符串区域开始
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
					if (DSLUtils.isParamBegin(b, c)) {
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

}
