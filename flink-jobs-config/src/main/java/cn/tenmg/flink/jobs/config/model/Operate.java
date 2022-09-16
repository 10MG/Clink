package cn.tenmg.flink.jobs.config.model;

/**
 * 操作配置接口
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
public interface Operate {

	/**
	 * 获取操作类型
	 * 
	 * @return 操作类型
	 */
	String getType();

	/**
	 * 获取处理结果另存为变量名
	 * 
	 * @return 处理结果另存为变量名
	 */
	String getSaveAs();

	/**
	 * 获取处理条件
	 * 
	 * @return 处理条件
	 */
	String getWhen();

}
