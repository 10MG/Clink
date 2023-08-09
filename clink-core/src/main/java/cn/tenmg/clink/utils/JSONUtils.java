package cn.tenmg.clink.utils;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * JSON工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public abstract class JSONUtils {

	/**
	 * 将参数集转化为JSON字符串
	 * 
	 * @param params
	 *            参数集
	 * @return 返回参数集的JSON字符串
	 */
	public static final String toJSONString(Map<String, Object> params) {
		if (params != null) {
			StringBuilder sb = new StringBuilder("{");
			boolean flag = false;
			for (Iterator<Entry<String, Object>> it = params.entrySet().iterator(); it.hasNext();) {
				Entry<String, Object> entry = it.next();
				Object value = entry.getValue();
				if (flag) {
					sb.append(", ");
				} else {
					flag = true;
				}
				appendKey(sb, entry.getKey());
				append(sb, value);
			}
			sb.append("}");
			return sb.toString();
		}
		return null;
	}

	/**
	 * 将参数集转化为JSON字符串
	 * 
	 * @param params
	 *            参数集
	 * @return 返回参数集的JSON字符串
	 */
	public static final String toJSONString(Collection<Object> params) {
		if (params != null) {
			StringBuilder sb = new StringBuilder("[");
			boolean flag = false;
			for (Iterator<Object> it = params.iterator(); it.hasNext();) {
				Object value = it.next();
				if (flag) {
					sb.append(", ");
				} else {
					flag = true;
				}
				append(sb, value);
			}
			sb.append("]");
			return sb.toString();
		}
		return null;
	}

	/**
	 * 将参数集转化为JSON字符串
	 * 
	 * @param params
	 *            参数集
	 * @return 返回参数集的JSON字符串
	 */
	public static final String toJSONString(Object... params) {
		if (params != null) {
			StringBuilder sb = new StringBuilder("[");
			for (int i = 0; i < params.length; i++) {
				Object value = params[i];
				if (i > 0) {
					sb.append(", ");
				}
				append(sb, value);
			}
			sb.append("]");
			return sb.toString();
		}
		return null;
	}

	private static final void appendKey(StringBuilder sb, String key) {
		sb.append("\"").append(key).append("\": ");
	}

	@SuppressWarnings("unchecked")
	private static final void append(StringBuilder sb, Object value) {
		if (value == null) {
			sb.append("null");
		} else {
			if (value instanceof String) {
				appendString(sb, (String) value);
			} else if (value instanceof Number || value instanceof Date || value instanceof Calendar
					|| value instanceof Boolean || value instanceof BigDecimal) {
				sb.append(value.toString());
			} else if (value instanceof Collection) {
				sb.append(toJSONString((Collection<Object>) value));
			} else if (value instanceof Object[]) {
				sb.append(toJSONString((Object[]) value));
			} else {
				appendString(sb, value.toString());
			}
		}
	}

	private static final void appendString(StringBuilder sb, String s) {
		sb.append("\"").append(s).append("\"");
	}
}
