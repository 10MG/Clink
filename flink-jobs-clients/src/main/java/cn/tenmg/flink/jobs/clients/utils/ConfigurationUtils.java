package cn.tenmg.flink.jobs.clients.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

/**
 * 配置工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public abstract class ConfigurationUtils {

	public static final String EMPTY_STRING = "";

	private static final char VALUE_BEGIN = '=', SINGLE_QUOTATION_MARK = '\'', BLANK_SPACE = ' ', BACKSLASH = '\\';

	private static final Set<Character> VALUE_END = new HashSet<Character>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 3151893719267729294L;

		{
			add(',');
			add('\r');
			add('\n');
		}
	};

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
			char a = BLANK_SPACE, b = BLANK_SPACE;
			boolean /* 是否在字符串区域 */ isString = false, isKey = true;
			StringBuilder key = new StringBuilder(), value = new StringBuilder();
			while (i < len) {
				char c = config.charAt(i);
				if (isString) {
					if (c == BACKSLASH) {
						backslashes++;
					} else {
						if (isStringEnd(a, b, c, backslashes)) {// 字符串区域结束
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
					if (c == SINGLE_QUOTATION_MARK) {// 字符串区域开始
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
						if (VALUE_END.contains(c)) {
							isKey = true;
							put(map, key, value);
							key.setLength(0);
							value.setLength(0);
							a = b;
							b = c;
							i++;
							for (; i < len; i++) {
								c = config.charAt(i);
								if (c > BLANK_SPACE) {
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
	 * 获取含有指定键的前缀的配置项的集合，该集合的键为去除该前缀后剩余的子串。
	 * 
	 * @param config
	 *            配置对象
	 * @param prefix
	 *            键的前缀
	 * @param camelCaseKey
	 *            是否将键转换为驼峰形式
	 * @return 指定键前缀的配置项的集合
	 */
	public static Properties getPrefixedKeyValuePairs(Properties config, String prefix, boolean camelCaseKey) {
		String key;
		Entry<Object, Object> entry;
		Properties keyValuePairs = new Properties();
		for (Iterator<Entry<Object, Object>> it = config.entrySet().iterator(); it.hasNext();) {
			entry = it.next();
			key = entry.getKey().toString();
			if (key.startsWith(prefix)) {
				keyValuePairs.put(StringUtils.toCamelCase(key.substring(prefix.length()), "[-|_]", false),
						entry.getValue());
			}
		}
		return keyValuePairs;
	}

	/**
	 * 根据优先键依次从配置中获取配置的属性，一旦某一键存在则返回其对应的值，均不存在则返回 {@code null}
	 * 
	 * @param config
	 *            配置
	 * @param priorKeys
	 *            优先键
	 * @return 配置属性值或 {@code null}
	 */
	public static String getProperty(Properties config, List<String> priorKeys) {
		return getProperty(config, priorKeys, null);
	}

	/**
	 * 根据优先键依次从配置中获取配置的属性，一旦某一键存在则返回其对应的值，均不存在则返回默认值
	 * 
	 * @param config
	 *            配置
	 * @param priorKeys
	 *            优先键
	 * @param defaultValue
	 *            默认值
	 * @return 配置属性值或默认值
	 */
	public static String getProperty(Properties config, List<String> priorKeys, String defaultValue) {
		String key;
		for (int i = 0, size = priorKeys.size(); i < size; i++) {
			key = priorKeys.get(i);
			if (config.containsKey(key)) {
				return config.getProperty(key);
			}
		}
		return defaultValue;
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

	/**
	 * 
	 * 根据指定的三个前后相邻字符a、b和c及当前字符c之前的连续反斜杠数量，判断其是否为命名参数脚本字符串区的结束位置
	 * 
	 * @param a
	 *            前第二个字符a
	 * @param b
	 *            前一个字符b
	 * @param c
	 *            当前字符c
	 * @param backslashes
	 *            当前字符c之前的连续反斜杠数量
	 * @return 是动态脚本字符串区域结束位置返回true，否则返回false
	 */
	private static boolean isStringEnd(char a, char b, char c, int backslashes) {
		if (c == SINGLE_QUOTATION_MARK) {
			if (b == BACKSLASH) {
				return backslashes % 2 == 0;
			} else {
				return true;
			}
		} else {
			return false;
		}
	}

	private static void put(Map<String, String> map, StringBuilder key, StringBuilder value) {
		String k = key.toString().trim(), v = value.toString().trim();
		int last = k.length() - 1;
		if (k.charAt(0) == SINGLE_QUOTATION_MARK && k.charAt(last) == SINGLE_QUOTATION_MARK) {
			k = k.substring(1, last);
		}
		map.put(k, v);
	}

}
