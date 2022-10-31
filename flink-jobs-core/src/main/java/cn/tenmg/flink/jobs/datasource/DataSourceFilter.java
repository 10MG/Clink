package cn.tenmg.flink.jobs.datasource;

import java.util.Map;

/**
 * 数据源过滤器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.0
 *
 */
public interface DataSourceFilter {
	/**
	 * 将数据源按照过滤规则过滤部分属性后返回
	 * 
	 * @param dataSource
	 *            数据源
	 */
	void doFilter(Map<String, String> dataSource);
}
