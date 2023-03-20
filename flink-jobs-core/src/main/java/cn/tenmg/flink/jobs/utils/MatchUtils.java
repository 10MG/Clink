package cn.tenmg.flink.jobs.utils;

import java.util.Collection;
import java.util.Iterator;

/**
 * 通配符匹配工具类。已废弃，请使用 {@code cn.tenmg.dsl.utils.MatchUtils} 替换
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.0
 *
 */
@Deprecated
public abstract class MatchUtils {

	private MatchUtils() {
	}

	/**
	 * 校验待比较的字符串是否与模板匹配（支持*作为通配符）
	 * 
	 * @param pattern
	 *            模板
	 * @param s
	 *            待比较的字符串
	 * @return 如果匹配则返回true，否则返回false
	 */
	public static boolean matches(String pattern, String s) {
		if (s == null) {
			return pattern == null;
		}
		return matchesSafeless(pattern, s);
	}

	/**
	 * 校验待比较的字符串是否与模板集中任一模板匹配（支持*作为通配符）
	 * 
	 * @param patterns
	 *            模板集
	 * @param s
	 *            待比较的字符串
	 * @return 如果匹配中任一模板则返回true，否则返回false
	 */
	public static boolean matchesAny(Collection<String> patterns, String s) {
		if (s == null) {
			return patterns == null;
		}
		String pattern;
		for (Iterator<String> it = patterns.iterator(); it.hasNext();) {
			pattern = it.next();
			if (pattern != null && matchesSafeless(pattern, s)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * 校验待比较的字符串是否与模板集中任一模板匹配（支持*作为通配符）
	 * 
	 * @param patterns
	 *            模板集
	 * @param s
	 *            待比较的字符串
	 * @return 如果匹配中任一模板则返回true，否则返回false
	 */
	public static boolean matchesAny(String[] patterns, String s) {
		if (s == null) {
			return patterns == null;
		}
		String pattern;
		for (int i = 0; i < patterns.length; i++) {
			pattern = patterns[i];
			if (pattern != null && matchesSafeless(pattern, s)) {
				return true;
			}
		}
		return false;
	}

	private static boolean matchesSafeless(String pattern, String s) {
		return s.matches(pattern.replaceAll("\\*", "[\\\\s\\\\S]*"));
	}

}
