package cn.tenmg.flink.jobs.operator.data.sync;

import java.util.Map;

/**
 * 列获取器
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public interface ColumnsGetter {

	/**
	 * 根据数据源、表名获取列名对数据类型的查找表
	 * 
	 * @param dataSource 数据源
	 * @param tableName  表名
	 * @return 列名对数据类型的查找表
	 * @throws Exception 发生异常
	 */
	Map<String, String> getColumns(Map<String, String> dataSource, String tableName) throws Exception;
}
