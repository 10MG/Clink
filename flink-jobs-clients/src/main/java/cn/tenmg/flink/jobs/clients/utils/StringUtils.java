package cn.tenmg.flink.jobs.clients.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * 字符串工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public abstract class StringUtils {

	private final static String EMPTY = "";

	public final static int INDEX_NOT_FOUND = -1;

	private static final String UNDERLINE = "_";

	/**
	 * 判断指定字符串是否为空（<code>null</code>）或者空字符串（<code>""</code>）
	 * 
	 * @param string
	 *            指定字符串
	 * @return 指定字符串为空（<code>null</code>）或者空字符串（<code>""</code>）返回<code>true</code>
	 *         ，否则返回<code>false</code>
	 */
	public static boolean isEmpty(String string) {
		return string == null || string.length() == 0;
	}

	/**
	 * 判断指定字符串非空（<code>null</code>）且非空字符串（<code>""</code>）
	 * 
	 * @param string
	 *            指定字符串
	 * @return 指定字符串非空（<code>null</code>）且非空字符串（<code>""</code>）返回<code>true</code>
	 *         ，否则返回<code>false</code>
	 */
	public static boolean isNotEmpty(String string) {
		return !isEmpty(string);
	}

	/**
	 * 判断指定字符串是否为空（<code>null</code>）、空字符串（<code>""</code>）或者仅含空格的字符串
	 * 
	 * @param string
	 *            指定字符串
	 * @return 指定字符串为空（<code>null</code>）、空字符串（<code>""</code>）或者仅含空格的字符串返回
	 *         <code>true</code>，否则返回<code>false</code>
	 */
	public static boolean isBlank(String string) {
		int len;
		if (string == null || (len = string.length()) == 0) {
			return true;
		}
		for (int i = 0; i < len; i++) {
			if ((Character.isWhitespace(string.charAt(i)) == false)) {
				return false;
			}
		}
		return true;
	}

	/**
	 * 判断指定字符串是否非空（<code>null</code>）、非空字符串（<code>""</code>）且非仅含空格的字符串
	 * 
	 * @param string
	 *            指定字符串
	 * @return 指定字符串非空（<code>null</code>）、非空字符串（<code>""</code>）且非仅含空格的字符串返回
	 *         <code>true</code>，否则返回<code>false</code>
	 */
	public static boolean isNotBlank(String string) {
		return !isBlank(string);
	}

	/**
	 * 清除指定字符串前后空格，为空（<code>null</code>）则返回空（<code>null</code>）
	 * 
	 * @param string
	 *            指定字符串
	 * @return 指定字符串为空（<code>null</code>）则返回空（<code>null</code>），否则返回清理前后空格后的字符串
	 */
	public static String trim(String string) {
		return string == null ? null : string.trim();
	}

	/**
	 * 清除指定字符串前后空格，为空（<code>null</code>）则返回空字符串（<code>""</code>）
	 * 
	 * @param string
	 *            指定字符串
	 * @return 指定字符串为空（<code>null</code>）返回空字符串（<code>""</code>），否则返回清理前后空格后的字符串
	 */
	public static String trimToEmpty(String string) {
		return string == null ? EMPTY : string.trim();
	}

	/**
	 * 清除指定字符串前后空格，为空（<code>null</code>）或者空字符串（<code>""</code>）则返回空（
	 * <code>null</code>）
	 * 
	 * @param string
	 *            指定字符串
	 * @return 指定字符串为空（<code>null</code>）或者空字符串（<code>""</code>）则返回空（
	 *         <code>null</code>），否则返回清理前后空格后的字符串
	 */
	public static String trimToNull(String string) {
		String r = trim(string);
		return isEmpty(r) ? null : r;
	}

	/**
	 * 删除指定字符串前导、后导字符
	 * 
	 * <pre>
	 * StringUtils.strip(null, *)          = null
	 * StringUtils.strip("", *)            = ""
	 * StringUtils.strip("abc", null)      = "abc"
	 * StringUtils.strip("  abc", null)    = "abc"
	 * StringUtils.strip("abc  ", null)    = "abc"
	 * StringUtils.strip(" abc ", null)    = "abc"
	 * StringUtils.strip("  abcyx", "xyz") = "  abc"
	 * </pre>
	 * 
	 * @param string
	 *            指定字符串
	 * @param stripChars
	 *            前导、后导字符集
	 * @return 如果指定字符串是否为空（null）或者空字符串（""）则直接返回该字符串，否则返回删除字符串首部、尾部所有前导、后导字符集（
	 *         <code>stripChars</code> ）中包含的字符后的字符串
	 */
	public static String strip(String string, String stripChars) {
		if (isEmpty(string)) {
			return string;
		}
		string = stripStart(string, stripChars);
		return stripEnd(string, stripChars);
	}

	/**
	 * 删除指定字符串前导字符
	 * 
	 * <pre>
	 * StringUtils.stripStart(null, *)          = null
	 * StringUtils.stripStart("", *)            = ""
	 * StringUtils.stripStart("abc", "")        = "abc"
	 * StringUtils.stripStart("abc", null)      = "abc"
	 * StringUtils.stripStart("  abc", null)    = "abc"
	 * StringUtils.stripStart("abc  ", null)    = "abc  "
	 * StringUtils.stripStart(" abc ", null)    = "abc "
	 * StringUtils.stripStart("yxabc  ", "xyz") = "abc  "
	 * </pre>
	 * 
	 * @param string
	 *            指定字符串
	 * @param stripChars
	 *            前导字符集
	 * @return 如果指定字符串为空（<code>stripChars</code>）则返回空（<code>null</code>
	 *         ），如果前导字符集（<code>stripChars</code>）为空（<code>null</code>
	 *         ）则返回删除所有前导空格后的字符串，否则返回删除字符串首部所有前导字符集（<code>stripChars</code>
	 *         ）中包含的字符后的字符串
	 */
	public static String stripStart(String string, String stripChars) {
		int strLen;
		if (string == null || (strLen = string.length()) == 0) {
			return string;
		}
		int start = 0;
		if (stripChars == null) {
			while ((start != strLen) && Character.isWhitespace(string.charAt(start))) {
				start++;
			}
		} else if (stripChars.length() == 0) {
			return string;
		} else {
			while ((start != strLen) && (stripChars.indexOf(string.charAt(start)) != INDEX_NOT_FOUND)) {
				start++;
			}
		}
		return string.substring(start);
	}

	/**
	 * 删除指定字符串后导字符
	 * 
	 * <pre>
	 * StringUtils.stripEnd(null, *)          = null
	 * StringUtils.stripEnd("", *)            = ""
	 * StringUtils.stripEnd("abc", "")        = "abc"
	 * StringUtils.stripEnd("abc", null)      = "abc"
	 * StringUtils.stripEnd("  abc", null)    = "  abc"
	 * StringUtils.stripEnd("abc  ", null)    = "abc"
	 * StringUtils.stripEnd(" abc ", null)    = " abc"
	 * StringUtils.stripEnd("  abcyx", "xyz") = "  abc"
	 * StringUtils.stripEnd("120.00", ".0")   = "12"
	 * </pre>
	 * 
	 * @param string
	 *            指定字符串
	 * @param stripChars
	 *            后导字符集
	 * @return 如果指定字符串为空（<code>stripChars</code>）则返回空（<code>null</code>
	 *         ），如果后导字符集（<code>stripChars</code>）为空（<code>null</code>
	 *         ）则返回删除所有后导空格后的字符串，否则返回删除字符串尾部所有后导字符集（<code>stripChars</code>
	 *         ）中包含的字符后的字符串
	 */
	public static String stripEnd(String string, String stripChars) {
		int end;
		if (string == null || (end = string.length()) == 0) {
			return string;
		}

		if (stripChars == null) {
			while ((end != 0) && Character.isWhitespace(string.charAt(end - 1))) {
				end--;
			}
		} else if (stripChars.length() == 0) {
			return string;
		} else {
			while ((end != 0) && (stripChars.indexOf(string.charAt(end - 1)) != INDEX_NOT_FOUND)) {
				end--;
			}
		}
		return string.substring(0, end);
	}

	/**
	 * 删除指定字符串数组中所有字符串的前导、后导空格
	 * 
	 * <pre>
	 * StringUtils.stripAll(null)             = null
	 * StringUtils.stripAll([])               = []
	 * StringUtils.stripAll(["abc", "  abc"]) = ["abc", "abc"]
	 * StringUtils.stripAll(["abc  ", null])  = ["abc", null]
	 * </pre>
	 * 
	 * @param strings
	 *            指定字符串
	 * @return 删除指定字符串数组中所有字符串的前导、后导空格后的新字符串数组
	 */
	public static String[] stripAll(String[] strings) {
		return stripAll(strings, null);
	}

	/**
	 * 删除指定字符串数组中所有字符串的前导、后导字符
	 * 
	 * <pre>
	 * StringUtils.stripAll(null, *)                = null
	 * StringUtils.stripAll([], *)                  = []
	 * StringUtils.stripAll(["abc", "  abc"], null) = ["abc", "abc"]
	 * StringUtils.stripAll(["abc  ", null], null)  = ["abc", null]
	 * StringUtils.stripAll(["abc  ", null], "yz")  = ["abc  ", null]
	 * StringUtils.stripAll(["yabcz", null], "yz")  = ["abc", null]
	 * </pre>
	 * 
	 * @param strings
	 *            指定字符串数组
	 * @param stripChars
	 *            前导、后导字符集
	 * @return 返回删除指定字符串数组中所有字符串的前导、后导字符后的新字符串数组
	 */
	public static String[] stripAll(String[] strings, String stripChars) {
		int strsLen;
		if (strings == null || (strsLen = strings.length) == 0) {
			return strings;
		}
		String[] newStrings = new String[strsLen];
		for (int i = 0; i < strsLen; i++) {
			newStrings[i] = strip(strings[i], stripChars);
		}
		return newStrings;
	}

	/**
	 * 比较两个字符串是否相等，同为空（<code>null</code>）也认为是相等的
	 * 
	 * @param string
	 *            字符串
	 * @param anotherString
	 *            另一个字符串
	 * @return 相等则返回<code>true</code>，否则返回<code>false</code>
	 */
	public static boolean equals(String string, String anotherString) {
		return string == null ? anotherString == null : string.equals(anotherString);
	}

	/**
	 * 忽略大小写，比较两个字符串是否每个字符都相同，同为空（<code>null</code>）也认为是相同的
	 * 
	 * @param string
	 *            字符串
	 * @param anotherString
	 *            另一个字符串
	 * @return 相同则返回<code>true</code>，否则返回<code>false</code>
	 */
	public static boolean equalsIgnoreCase(String string, String anotherString) {
		return string == null ? anotherString == null : string.equalsIgnoreCase(anotherString);
	}

	/**
	 * 查找指定字符在指定字符串中第一次出现的位置
	 * 
	 * @param string
	 *            指定字符串
	 * @param ch
	 *            指定字符
	 * @return 指定字符串为空（<code>null</code>）或者没有找到返回<code>-1</code>，否则返回字符第一次出现的所在位置（从
	 *         <code>0</code>开始）
	 */
	public static int indexOf(String string, char ch) {
		if (isEmpty(string)) {
			return INDEX_NOT_FOUND;
		}
		return string.indexOf(ch);
	}

	/**
	 * 从指定位置开始查找指定字符在指定字符串中的位置
	 * 
	 * @param string
	 *            指定字符串
	 * @param ch
	 *            指定字符
	 * @param fromIndex
	 *            指定位置
	 * @return 字符串为空（<code>null</code>）或者没有找到返回<code>-1</code>
	 *         ，否则返回字符第一次出现的所在位置（从<code>0</code>开始）
	 */
	public static int indexOf(String string, char ch, int fromIndex) {
		if (isEmpty(string)) {
			return INDEX_NOT_FOUND;
		}
		return string.indexOf(ch, fromIndex);
	}

	/**
	 * 查找指定子串在指定字符串中第一次出现的位置
	 * 
	 * @param string
	 *            指定字符串
	 * @param searchString
	 *            指定子串
	 * @return 字符串或子串为空（<code>null</code>）或者字符串中没有找到子串则返回<code>-1</code>
	 *         ，否则返回字符串第一次出现的位置（从<code>0</code>开始）
	 */
	public static int indexOf(String string, String searchString) {
		if (string == null || searchString == null) {
			return INDEX_NOT_FOUND;
		}
		return string.indexOf(searchString);
	}

	/**
	 * 从指定位置开始查找指定子串在指定字符串中第一次出现的位置
	 * 
	 * @param string
	 *            指定字符串
	 * @param searchString
	 *            指定子串
	 * @param fromIndex
	 *            指定位置
	 * @return 指定字符串或指定子串为空（<code>null</code>）或者指定字符串中没有找到子串则返回<code>-1</code>
	 *         ，否则返回指定字符串从<code>fromIndex</code>开始查找第一次出现的位置（从<code>0</code>开始）
	 */
	public static int indexOf(String string, String searchString, int fromIndex) {
		if (string == null || searchString == null) {
			return INDEX_NOT_FOUND;
		}
		// JDK1.2/JDK1.3 have a bug, when fromIndex > string.length for "",
		// hence
		if (searchString.length() == 0 && fromIndex >= string.length()) {
			return string.length();
		}
		return string.indexOf(searchString, fromIndex);
	}

	/**
	 * 忽略大小写，查找子串在字符串中的位置
	 * 
	 * <pre>
	 * StringUtils.indexOfIgnoreCase(null, *)          = -1
	 * StringUtils.indexOfIgnoreCase(*, null)          = -1
	 * StringUtils.indexOfIgnoreCase(*, "")            = 0
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "a")  = 0
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "b")  = 2
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "ab") = 1
	 * </pre>
	 * 
	 * @param string
	 *            待查找的字符串
	 * @param searchString
	 *            需要查找的子串
	 * @return 字符串或者子串为空（<code>null</code>），则返回<code>-1</code>；子串为空字符串（
	 *         <code>""</code>）,则返回<code>0</code>；其他情况下，找到则返回其第一次出现的位置，否则返回
	 *         <code>-1</code>
	 */
	public static int indexOfIgnoreCase(String string, String searchString) {
		return indexOfIgnoreCase(string, searchString, 0);
	}

	/**
	 * 忽略大小写，从指定位置开始查找子串在字符串中的位置
	 * 
	 * <pre>
	 * StringUtils.indexOfIgnoreCase(null, *, *)                        = -1
	 * StringUtils.indexOfIgnoreCase(*, null, *)                        = -1
	 * StringUtils.indexOfIgnoreCase(not null, "", -*)                  = 0
	 * StringUtils.indexOfIgnoreCase(not null, "", 0)                   = 0
	 * StringUtils.indexOfIgnoreCase(not null, "", 小于等于string.length + 1) = formIndex
	 * StringUtils.indexOfIgnoreCase(not null, "", 大于string.length)      = -1
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "A", 0)                = 0
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "B", 0)                = 2
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "AB", 0)               = 1
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "B", 3)                = 5
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "B", 9)                = -1
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "B", -1)               = 2
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "", 2)                 = 2
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "", 8)                 = 8
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "", 9)                 = 9
	 * StringUtils.indexOfIgnoreCase("aabaabaa", "", 10)                = -1
	 * </pre>
	 * 
	 * @param string
	 *            待查找的字符串
	 * @param searchString
	 *            需要查找的子串
	 * @param formIndex
	 *            查找起始位置（大于等于<code>0</code>，负数和<code>0</code>等价）
	 * @return 字符串或者子串为空（<code>null</code>），则返回<code>-1</code>；字符串非空、子串为空字符串（
	 *         <code>""</code>）且查找起始位置为非正整数,则返回<code>0</code>；字符串非空、子串为空字符串（
	 *         <code>""</code>）且查找起始位置<code>formIndex</code>小于等于
	 *         <code>string.length + 1</code>,则返回<code>formIndex</code>
	 *         ；其他情况下，找到则返回其第一次出现的位置，否则返回 <code>-1</code>
	 */
	public static int indexOfIgnoreCase(String string, String searchString, int formIndex) {
		if (string == null || searchString == null) {
			return INDEX_NOT_FOUND;
		}
		if (formIndex < 0) {
			formIndex = 0;
		}
		int endLimit = (string.length() - searchString.length()) + 1;
		if (formIndex > endLimit) {
			return INDEX_NOT_FOUND;
		}
		if (searchString.length() == 0) {
			return formIndex;
		}
		for (int i = formIndex; i < endLimit; i++) {
			if (string.regionMatches(true, i, searchString, 0, searchString.length())) {
				return i;
			}
		}
		return INDEX_NOT_FOUND;
	}

	/**
	 * 查找子串在字符串中第<code>ordinal</code>次出现的位置
	 * 
	 * <pre>
	 * StringUtils.ordinalIndexOf(null, *, *)          = -1
	 * StringUtils.ordinalIndexOf(*, null, *)          = -1
	 * StringUtils.ordinalIndexOf(*, *, -*)            = -1
	 * StringUtils.ordinalIndexOf("", "", *)           = 0
	 * StringUtils.ordinalIndexOf("aabaabaa", "a", 1)  = 0
	 * StringUtils.ordinalIndexOf("aabaabaa", "a", 2)  = 1
	 * StringUtils.ordinalIndexOf("aabaabaa", "b", 1)  = 2
	 * StringUtils.ordinalIndexOf("aabaabaa", "b", 2)  = 5
	 * StringUtils.ordinalIndexOf("aabaabaa", "ab", 1) = 1
	 * StringUtils.ordinalIndexOf("aabaabaa", "ab", 2) = 4
	 * StringUtils.ordinalIndexOf("aabaabaa", "", 1)   = 0
	 * StringUtils.ordinalIndexOf("aabaabaa", "", 2)   = 0
	 * </pre>
	 * 
	 * @param string
	 *            待查找字符串
	 * @param searchString
	 *            查找的字符串
	 * @param ordinal
	 *            序数（从<code>1</code>开始）
	 * @return 字符串、子串为空（<code>null</code>）或者序数为非正数时，返回<code>-1</code> ；字符串和子串均为空字符串（
	 *         <code>""</code>）时，返回<code>0</code>；查找子串为空字符串（
	 *         <code>""</code>）、字符串不为空（<code>null</code>）且序数为正数时，返回 <code>0</code>；
	 *         其他情况，能找到子串的返回其对应位置，否则返回 <code>-1</code>
	 */
	public static int ordinalIndexOf(String string, String searchString, int ordinal) {
		return ordinalIndexOf(string, searchString, ordinal, false);
	}

	/**
	 * 查找子串在字符串中第<code>ordinal</code>次出现的位置
	 * 
	 * <pre>
	 * StringUtils.ordinalIndexOf(null, *, *, *)              = -1
	 * StringUtils.ordinalIndexOf(*, null, *, *)              = -1
	 * StringUtils.ordinalIndexOf(*, *, -*, *)                = -1
	 * StringUtils.ordinalIndexOf("", "", *, *)               = 0
	 * StringUtils.ordinalIndexOf("aabaabaa", "a", 1, false)  = 0
	 * StringUtils.ordinalIndexOf("aabaabaa", "a", 2, false)  = 1
	 * StringUtils.ordinalIndexOf("aabaabaa", "b", 1, false)  = 2
	 * StringUtils.ordinalIndexOf("aabaabaa", "b", 2, false)  = 5
	 * StringUtils.ordinalIndexOf("aabaabaa", "ab", 1, false) = 1
	 * StringUtils.ordinalIndexOf("aabaabaa", "ab", 2, false) = 4
	 * StringUtils.ordinalIndexOf("aabaabaa", "", 1, false)   = 0
	 * StringUtils.ordinalIndexOf("aabaabaa", "", 2, false)   = 0
	 * 
	 * StringUtils.ordinalIndexOf("aabaabaa", "a", 1, true)  = 7
	 * StringUtils.ordinalIndexOf("aabaabaa", "a", 2, true)  = 6
	 * StringUtils.ordinalIndexOf("aabaabaa", "b", 1, true)  = 5
	 * StringUtils.ordinalIndexOf("aabaabaa", "b", 2, true)  = 2
	 * StringUtils.ordinalIndexOf("aabaabaa", "ab", 1, true) = 4
	 * StringUtils.ordinalIndexOf("aabaabaa", "ab", 2, true) = 1
	 * StringUtils.ordinalIndexOf("aabaabaa", "", 1, true)   = 8
	 * StringUtils.ordinalIndexOf("aabaabaa", "", 2, true)   = 8
	 * </pre>
	 * 
	 * @param string
	 *            待查找字符串
	 * @param searchString
	 *            查找的字符串
	 * @param ordinal
	 *            序数（从<code>1</code>开始）
	 * @param reverse
	 *            是否逆序查找
	 * @return 字符串、子串为空（<code>null</code>）或者序数为非正数时，返回<code>-1</code> ；字符串和子串均为空字符串（
	 *         <code>""</code>）时，返回<code>0</code>；正序查找子串为空字符串（
	 *         <code>""</code>）、字符串不为空（<code>null</code>）且序数为正数时，返回 <code>0</code>；
	 *         逆序查找子串为空字符串（ <code>""</code>）、字符串不为空（ <code>null</code>
	 *         ）且序数为正数时，返回字符串的长度；其他情况，能找到子串的返回其对应位置，否则返回 <code>-1</code>
	 */
	public static int ordinalIndexOf(String string, String searchString, int ordinal, boolean reverse) {
		if (string == null || searchString == null || ordinal <= 0) {
			return INDEX_NOT_FOUND;
		}
		if (searchString.length() == 0) {
			return reverse ? string.length() : 0;
		}
		int found = 0;
		int index = reverse ? string.length() : INDEX_NOT_FOUND;
		do {
			if (reverse) {
				index = string.lastIndexOf(searchString, index - 1);
			} else {
				index = string.indexOf(searchString, index + 1);
			}
			if (index < 0) {
				return index;
			}
			found++;
		} while (found < ordinal);
		return index;
	}

	/**
	 * 将指定字符串转换为整数列表
	 * 
	 * @param string
	 *            指定字符串
	 * @param regex
	 *            分割符，可以是正则表达式
	 * @return 返回转后的整数列表。如果指定字符串或者分割符为<code>null</code>则返回<code>null</code>
	 */
	public static List<Integer> toIntegerList(String string, String regex) {
		if (StringUtils.isBlank(string) || StringUtils.isBlank(regex)) {
			return null;
		}
		List<Integer> result = new ArrayList<Integer>();
		String[] strings = string.split(regex);
		for (String s : strings) {
			result.add(Integer.valueOf(s));
		}
		return result;
	}

	/**
	 * 将指定字符串转换为字符串列表
	 * 
	 * @param string
	 *            指定字符串
	 * @param regex
	 *            分割符，可以是正则表达式
	 * @return 返回转后的整数列表。如果指定字符串或者分割符为<code>null</code>则返回<code>null</code>
	 */
	public static List<String> toStringList(String string, String regex) {
		if (StringUtils.isBlank(string) || StringUtils.isBlank(regex)) {
			return null;
		}
		List<String> result = new ArrayList<String>();
		String[] strings = string.split(regex);
		for (String s : strings) {
			result.add(s);
		}
		return result;
	}

	/**
	 * 判断指定字符串是否为数字
	 * 
	 * @param string
	 *            指定字符串
	 * @return 指定字符串为数字则返回<code>true</code>，否则返回<code>false</code>
	 */
	public static boolean isNumber(String string) {
		Pattern pattern = Pattern.compile("[0-9]*");
		return pattern.matcher(string).matches();
	}

	/**
	 * 将字符串转换为驼峰形式的字符串
	 * 
	 * @param string
	 *            原字符串
	 * @param regex
	 *            分隔符(可为正则表达式，无则为null)
	 * @param isUpperCase
	 *            是否首字母大写
	 * @return 返回驼峰形式的字符串
	 */
	public static String toCamelCase(String string, String regex, boolean isUpperCase) {
		if (StringUtils.isBlank(string)) {
			return string;
		} else {
			if (regex == null) {
				string = (isUpperCase ? string.substring(0, 1).toUpperCase() : string.substring(0, 1).toLowerCase())
						.concat(string.substring(1));
			} else {
				String items[] = string.split(regex), item = items[0];
				string = (isUpperCase ? item.substring(0, 1).toUpperCase() : item.substring(0, 1).toLowerCase())
						.concat(item.substring(1).toLowerCase());
				for (int i = 1; i < items.length; i++) {
					item = items[i];
					string = string.concat(item.substring(0, 1).toUpperCase()).concat(item.substring(1).toLowerCase());
				}
			}
		}
		return string;
	}

	/**
	 * 将字符串反转并返回
	 * 
	 * @param string
	 *            字符串
	 * @return 返回反转后的字符串
	 */
	public static String reverse(String string) {
		if (string == null) {
			return string;
		}
		return new StringBuilder(string).reverse().toString();
	}

	/**
	 * 将驼峰表示的字符串转为使用按下划线分割的字符串
	 * 
	 * @param s
	 *            原字符串
	 * @return 如果s为null则返回null，否则返回转换后的使用按下划线分割的字符串
	 */
	public static String camelToUnderline(String s) {
		if (s == null) {
			return null;
		}
		int len = s.length();
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			char c = s.charAt(i);
			if (Character.isUpperCase(c)) {
				if (i > 0) {
					sb.append(UNDERLINE);
				}
				sb.append(Character.toLowerCase(c));
			} else {
				sb.append(c);
			}
		}
		return sb.toString();
	}

	/**
	 * 将驼峰表示的字符串转为使用按下划线分割的字符串并转换为大写或小写
	 * 
	 * @param s
	 *            原字符串
	 * @return 如果s为null则返回null，否则返回转换后的使用按下划线分割的字符串的大写或小写
	 */
	public static String camelToUnderline(String s, boolean upperOrLowerCace) {
		if (s == null) {
			return null;
		}
		int len = s.length();
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			char c = s.charAt(i);
			if (Character.isUpperCase(c) && i > 0) {
				sb.append(UNDERLINE);
			}
			if (upperOrLowerCace) {
				sb.append(Character.toUpperCase(c));
			} else {
				sb.append(Character.toLowerCase(c));
			}
		}
		return sb.toString();
	}

	/**
	 * 拼接字符串
	 * 
	 * @param args
	 *            拼接的对象
	 * @return 各对象拼接后的字符串
	 */
	public static String concat(Object... args) {
		return concatInner(args);
	}

	/**
	 * 拼接字符串
	 * 
	 * @param args
	 *            拼接的字符串
	 * @return 各对象拼接后的字符串
	 */
	public static String concat(String... args) {
		return concatInner(args);
	}

	/**
	 * 拼接字符串
	 * 
	 * @param args
	 *            拼接的字符序列
	 * @return 各对象拼接后的字符串
	 */
	public static String concat(CharSequence... args) {
		return concatInner(args);
	}

	@SuppressWarnings("unchecked")
	private static <T extends Object> String concatInner(T... args) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < args.length; i++) {
			sb.append(args[i]);
		}
		return sb.toString();
	}
}
