package cn.tenmg.clink.datasource.filter;

/**
 * 汇（Sink）数据源过滤器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.0
 */
public class SinkDataSourceFilter extends AbstractDataSourceFilter {

	private static final String KEY_PREFIX = "sink.datasource.filter.";

	@Override
	String getKeyPrefix() {
		return KEY_PREFIX;
	}

}
