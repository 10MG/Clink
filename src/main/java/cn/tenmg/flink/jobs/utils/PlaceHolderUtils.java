package cn.tenmg.flink.jobs.utils;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.tenmg.dsl.utils.StringUtils;

/**
 * 占位符工具类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.1
 */
public abstract class PlaceHolderUtils {

	private static final Pattern paramPattern = Pattern.compile("\\$\\{[^}]+\\}"),
			arrayPattern = Pattern.compile("\\[[^\\]]+\\]");

	/**
	 * 将模板字符串中占位符替换为指定的参数
	 * 
	 * @param tpl
	 *            模板字符串
	 * @param params
	 *            参数集（分别列出参数名和参数值）
	 * @return 返回将模板字符串中占位符替换为指定的参数后的字符串
	 */
	public static String replace(String tpl, CharSequence... params) {
		if (params != null && params.length > 1) {
			Map<CharSequence, CharSequence> map = new HashMap<CharSequence, CharSequence>();
			for (int i = 0; i < params.length; i += 2) {
				map.put(params[i], params[i + 1]);
			}
			return replace(tpl, map);
		} else {
			return tpl;
		}
	}

	/**
	 * 将模板字符串中占位符替换为指定的参数
	 * 
	 * @param tpl
	 *            模板字符串
	 * @param params
	 *            参数集
	 * @return 返回将模板字符串中占位符替换为指定的参数后的字符串
	 */
	public static String replace(String tpl, Map<?, ?> params) {
		if (StringUtils.isBlank(tpl)) {
			return tpl;
		} else if (params == null || params.isEmpty()) {
			return tpl;
		} else {
			StringBuffer sb = new StringBuffer();
			Matcher m = paramPattern.matcher(tpl);
			String name;
			Object value;
			while (m.find()) {
				name = m.group();
				value = getParam(params, name.substring(2, name.length() - 1));
				if (value != null) {
					m.appendReplacement(sb, value.toString());
				} else {
					m.appendReplacement(sb, "");
				}
			}
			m.appendTail(sb);
			return sb.toString();
		}
	}

	/**
	 * 获取参数集中的参数值。参数名支持使用“key.name”访问参数值的属性值，层级数不限，支持使用“[*]”访问数组值，维数不限，“key.name”和“[*]”也可以配合使用
	 * 
	 * @param params
	 *            参数集
	 * @param name
	 *            参数名
	 * @return 如果参数集的参数存在则，返回它的值；否则，返回null
	 */
	public static Object getParam(Map<?, ?> params, String name) {
		Object value = params.get(name);
		if (value == null) {
			if (name.contains(".")) {// 访问Bean或者Map属性
				String[] names = name.split("\\.");
				name = names[0];
				value = params.get(name);
				if (value == null) {// 如果类似“key.name[*]……”形式的，可能是访问数组的某一项值或者是访问Map对象的属性值。如果是，则获取数组的某一项值或者Map对象的某个属性值
					return getMaybeArrayOrMapValue(params, name);
				} else {
					for (int i = 1; i < names.length; i++) {
						name = names[i];
						value = ObjectUtils.getValue(value, name);// 获取对象属性
						if (value == null) {// 可能是数组
							Matcher m = arrayPattern.matcher(name);
							if (m.find()) {// 含有数组访问符“[]”
								value = ObjectUtils.getValue(value, name.substring(0, name.indexOf("[")));// 获取数组对象
								if (value == null) {// 数组对象为null，返回null
									return null;
								} else {// 否则，获取数组的值
									value = getArrayOrMapValue(value, params, m);
								}
							}
							return value;
						}
					}
					return value;
				}
			} else {// 如果类似“key[*]……”形式的，可能是访问数组的某一项值或者是访问Map对象的属性值。如果是，则获取数组的某一项值或者Map对象的某个属性值
				return getMaybeArrayOrMapValue(params, name);
			}
		} else {
			return value;
		}
	}

	/**
	 * 获取可能通过“[*]”方式访问的数组某一项值或Map对象属性值
	 * 
	 * @param params
	 *            参数集
	 * @param name
	 *            参数名
	 * @return 如果含有“[*]”符号，则获取数组的值或者Map对象的属性值；否则返回null
	 */
	private static final Object getMaybeArrayOrMapValue(Map<?, ?> params, String name) {
		Object value = null;
		Matcher m = arrayPattern.matcher(name);
		if (m.find()) {
			value = params.get(name.substring(0, name.indexOf("[")));
			if (value == null) {
				return null;
			} else {// 继续获取下一维数组的值
				value = getArrayOrMapValue(value, params, m);
			}
		}
		return value;
	}

	private static final Object getArrayOrMapValue(Object value, Map<?, ?> params, Matcher m) {
		value = getArrayOrMapValue(value, params, m.group());
		while (value != null && m.find()) {
			value = getArrayOrMapValue(value, params, m.group());
		}
		return value;
	}

	private static final Object getArrayOrMapValue(Object value, Map<?, ?> params, String group) {
		String name = group.substring(1, group.length() - 1);
		return getValue(value, params, name);
	}

	private static final Object getValue(Object value, Map<?, ?> params, String name) {
		Object v = params.get(name);
		String real = name;
		if (v != null) {
			real = v.toString();
		}
		if (value instanceof Map) {
			return ((Map<?, ?>) value).get(real);
		} else if (value instanceof List) {
			return ((List<?>) value).get(Integer.valueOf(real));
		} else if (value instanceof Object[]) {
			return ((Object[]) value)[Integer.valueOf(real)];
		} else if (value instanceof LinkedHashSet) {
			return ((LinkedHashSet<?>) value).toArray()[Integer.valueOf(real)];
		} else {
			return null;
		}
	}
}
