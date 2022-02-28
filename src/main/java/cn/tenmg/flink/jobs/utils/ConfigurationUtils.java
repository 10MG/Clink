package cn.tenmg.flink.jobs.utils;

import java.util.HashMap;
import java.util.Map;

import cn.tenmg.dsl.utils.DSLUtils;

/**
 * 配置工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public abstract class ConfigurationUtils {

	public static final String EMPTY_STRING = "";

	private static final char VALUE_BEGIN = '=', VALUE_END = ',';

	/**
	 * 加载字符串配置
	 * 
	 * @param config
	 *            字符串配置
	 * @return 返回配置查找表
	 */
	public static Map<String, String> load(String config) {
		if (config == null) {
			return null;
		} else {
			Map<String, String> map = new HashMap<String, String>();
			config = config.trim();
			int len = config.length(), i = 0, backslashes = 0;
			char a = DSLUtils.BLANK_SPACE, b = DSLUtils.BLANK_SPACE;
			boolean /* 是否在字符串区域 */ isString = false, isKey = true;
			StringBuilder key = new StringBuilder(), value = new StringBuilder();
			while (i < len) {
				char c = config.charAt(i);
				if (isString) {
					if (c == DSLUtils.BACKSLASH) {
						backslashes++;
					} else {
						if (DSLUtils.isStringEnd(a, b, c, backslashes)) {// 字符串区域结束
							isString = false;
						}
						backslashes = 0;
					}
					if (isKey) {
						key.append(c);
					} else {
						value.append(c);
					}
				} else {
					if (c == DSLUtils.SINGLE_QUOTATION_MARK) {// 字符串区域开始
						isString = true;
						if (isKey) {
							key.append(c);
						} else {
							value.append(c);
						}
					} else if (isKey) {
						if (c == VALUE_BEGIN) {
							isKey = false;
						} else {
							key.append(c);
						}
					} else {
						if (c == VALUE_END) {
							isKey = true;
							put(map, key, value);
							key.setLength(0);
							value.setLength(0);
							a = b;
							b = c;
							i++;
							for (; i < len; i++) {
								c = config.charAt(i);
								if (c > DSLUtils.BLANK_SPACE) {
									break;
								}
								a = b;
								b = c;
							}
							continue;
						} else {
							value.append(c);
						}
					}
				}
				a = b;
				b = c;
				i++;
			}
			if (key.length() > 0) {
				put(map, key, value);
			}
			return map;
		}
	}

	/**
	 * 判断一个数据源是否为JDBC
	 * 
	 * @param dataSource
	 *            数据源
	 * @return 如果该数据源连接器connector为jdbc则返回true否则返回false
	 */
	public static boolean isJDBC(Map<String, String> dataSource) {
		return "jdbc".equals(dataSource.get("connector"));
	}

	/**
	 * 判断一个数据源是否为Kafka
	 * 
	 * @param dataSource
	 *            数据源
	 * @return 如果该数据源连接器connector为kafka则返回true否则返回false
	 */
	public static boolean isKafka(Map<String, String> dataSource) {
		return "kafka".equals(dataSource.get("connector"));
	}

	private static void put(Map<String, String> map, StringBuilder key, StringBuilder value) {
		String k = key.toString().trim(), v = value.toString().trim();
		int last = k.length() - 1;
		if (k.charAt(0) == DSLUtils.SINGLE_QUOTATION_MARK && k.charAt(last) == DSLUtils.SINGLE_QUOTATION_MARK) {
			k = k.substring(1, last);
		}
		map.put(k, v);
	}

}
