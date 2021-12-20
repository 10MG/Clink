package cn.tenmg.flink.jobs.operator.support;

import java.util.HashSet;
import java.util.Set;

import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.model.Operate;
import cn.tenmg.flink.jobs.operator.AbstractOperator;

/**
 * 支持SQL保留关键字的操作器抽象类
 * 
 * @author June wjzhao@aliyun.com
 *
 * @param <T>
 *            操作类型
 * @since 1.1.5
 */
public abstract class SqlReservedKeywordSupport<T extends Operate> extends AbstractOperator<T> {

	protected static final String SQL_RESERVED_KEYWORD_WRAP_PREFIX = "`", SQL_RESERVED_KEYWORD_WRAP_SUFFIX = "`";

	protected static final Set<String> sqlReservedKeywords = new HashSet<String>();

	static {
		String keywords = FlinkJobsContext.getProperty("sql.reserved.keywords");
		if (StringUtils.isNotBlank(keywords)) {
			String[] words = keywords.split(",");
			for (int i = 0; i < words.length; i++) {
				sqlReservedKeywords.add(words[i].trim().toUpperCase());
			}
		}
	}

	protected static String wrapIfReservedKeywords(String word) {
		if (word != null && sqlReservedKeywords.contains(word.toUpperCase())) {
			return SQL_RESERVED_KEYWORD_WRAP_PREFIX + word + SQL_RESERVED_KEYWORD_WRAP_SUFFIX;
		}
		return word;
	}

}