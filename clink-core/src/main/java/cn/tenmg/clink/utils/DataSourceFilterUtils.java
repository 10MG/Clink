package cn.tenmg.clink.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import cn.tenmg.clink.datasource.DataSourceFilter;
import cn.tenmg.clink.exception.IllegalConfigurationException;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 数据源过滤器工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.0
 */
public abstract class DataSourceFilterUtils {

	private static final Map<String, DataSourceFilter> filters = MapUtils.newHashMap();

	static {
		DataSourceFilter filter;
		ServiceLoader<DataSourceFilter> loader = ServiceLoader.load(DataSourceFilter.class);
		for (Iterator<DataSourceFilter> it = loader.iterator(); it.hasNext();) {
			filter = it.next();
			filters.put(StringUtils.toCamelCase(filter.getClass().getSimpleName().replace("DataSourceFilter", ""), null,
					false), filter);
		}
	}

	/**
	 * 根据数据源过滤器名称获取数据源过滤器
	 * 
	 * @param name
	 *            数据源过滤器名称
	 * @return 数据源过滤器
	 */
	public static DataSourceFilter getDataSourceFilter(String name) {
		DataSourceFilter filter = filters.get(name);
		if (filter == null) {
			try {
				filter = (DataSourceFilter) Class.forName(name).getConstructor().newInstance();
			} catch (Exception e) {
				throw new IllegalConfigurationException("Failed to load DataSourceFilter " + name, e);
			}
		}
		return filter;
	}

	/**
	 * 对数据源使用指定名称的数据源过滤器，并返回过滤后的数据源
	 * 
	 * @param name
	 *            过滤器的名称
	 * @param dataSource
	 *            数据源
	 * @return 过滤后的数据源
	 */
	public static Map<String, String> filter(String name, Map<String, String> dataSource) {
		if (StringUtils.isBlank(name)) {
			return dataSource;
		}
		HashMap<String, String> newDataSource = MapUtils.toHashMap(dataSource);
		getDataSourceFilter(name).doFilter(newDataSource);
		return newDataSource;

	}
}
