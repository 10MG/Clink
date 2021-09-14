package cn.tenmg.flink.jobs.operator.data.sync;

import java.util.Map;

import cn.tenmg.flink.jobs.utils.JDBCUtils;

/**
 * 列获取器工厂
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public abstract class ColumnsGetterFactory {

	/**
	 * 根据使用的数据源获取列获取器实例
	 * 
	 * @param dataSource 数据源
	 * @return 返回列获取器实例
	 */
	public static ColumnsGetter getAssetsInventory(Map<String, String> dataSource) {
		String product = JDBCUtils.getProduct(dataSource.get("url"));
		return null;
	}
}
