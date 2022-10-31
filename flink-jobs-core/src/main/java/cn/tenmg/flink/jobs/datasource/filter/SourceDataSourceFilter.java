package cn.tenmg.flink.jobs.datasource.filter;

/**
 * 源（Source）数据源过滤器。
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.0
 * 
 */
public class SourceDataSourceFilter extends AbstractDataSourceFilter {

	private static final String KEY_PREFIX = "source.datasource.filter.";

	@Override
	String getKeyPrefix() {
		return KEY_PREFIX;
	}

}
