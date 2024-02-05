package cn.tenmg.flink.jobs.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.ObjectUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 占位符工具类。已废弃，请使用 {@code cn.tenmg.dsl.utils.PlaceHolderUtils} 替换
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.1
 */
@Deprecated
public abstract class PlaceHolderUtils {

	private static final Pattern PARAM_PATTERN = Pattern.compile("\\$\\{[^}]+\\}");

	private static final HashSet<Character> ENCODE_CHARACTERS = new HashSet<Character>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = -1974796098018071303L;

		{
			add('$');
		}
	};

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
			Matcher m = PARAM_PATTERN.matcher(tpl);
			String name, s;
			Object value;
			boolean encode = false;
			while (m.find()) {
				name = m.group();
				value = ObjectUtils.getValueIgnoreException(params, name.substring(2, name.length() - 1));
				if (value == null) {
					m.appendReplacement(sb, "");
				} else {
					s = value.toString();
					char c;
					StringBuilder encoded = new StringBuilder();
					for (int i = 0, len = s.length(); i < len; i++) {
						c = s.charAt(i);
						if (c == DSLUtils.BACKSLASH) {
							encode = true;
							encoded.append(DSLUtils.BACKSLASH).append(DSLUtils.BACKSLASH).append(DSLUtils.BACKSLASH)
									.append(DSLUtils.BACKSLASH);
						} else {
							if (ENCODE_CHARACTERS.contains(c)) {
								encode = true;
								encoded.append(DSLUtils.BACKSLASH);
							}
							encoded.append(c);
						}
					}
					m.appendReplacement(sb, encoded.toString());
				}
			}
			if (encode) {
				StringBuffer uncode = new StringBuffer();
				int i = 0, len = sb.length();
				char c, n;
				while (i < len) {
					c = sb.charAt(i);
					if (++i < len && c == DSLUtils.BACKSLASH) {
						n = sb.charAt(i);
						if (n == DSLUtils.BACKSLASH) {
							i++;
							uncode.append(c);
						} else if (!ENCODE_CHARACTERS.contains(n)) {
							uncode.append(c);
						}
					} else {
						uncode.append(c);
					}
				}
				sb = uncode;
			}
			m.appendTail(sb);
			return sb.toString();
		}
	}
}
