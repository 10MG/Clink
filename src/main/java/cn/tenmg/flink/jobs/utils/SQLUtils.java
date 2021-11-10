package cn.tenmg.flink.jobs.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import cn.tenmg.dsl.utils.DSLUtils;

/**
 * SQL工具类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public abstract class SQLUtils {

	public static final String SINGLE_QUOTATION_MARK = "'", SPACE_EQUALS_SPACE = " = ";

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

}
