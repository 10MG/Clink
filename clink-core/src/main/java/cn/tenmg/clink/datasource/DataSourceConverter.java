package cn.tenmg.clink.datasource;

import java.util.Map;

/**
 * 数据源转换器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public interface DataSourceConverter {

	/**
	 * 获取适用的连接器
	 * 
	 * @return 适用的连接器
	 */
	String connector();

	/**
	 * 根据表名对数据源进行转换
	 * 
	 * @param dataSource
	 *            数据源
	 * @param table
	 *            表名
	 * @return 转换后的数据源
	 */
	Map<String, String> convert(Map<String, String> dataSource, String table);

}
