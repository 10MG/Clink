package cn.tenmg.clink.config.model.data.sync;

import java.io.Serializable;

/**
 * 同步数据列
 * 
 * @author cbb 2545095524@qq.com
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
public interface Column extends Serializable {

	/**
	 * 获取来源列名
	 * 
	 * @return 来源列名
	 */
	String getFromName();

	/**
	 * 设置来源列名
	 * 
	 * @param fromName
	 *            来源列名
	 */
	void setFromName(String fromName);

	/**
	 * 获取来源列数据类型
	 * 
	 * @return 来源列数据类型
	 */
	String getFromType();

	/**
	 * 设置来源列数据类型。如果缺省，则如果开启智能模式会自动获取目标数据类型作为来源数据类型，如果关闭智能模式则必填
	 * 
	 * @param fromType
	 *            来源列数据类型
	 */
	void setFromType(String fromType);

	/**
	 * 获取目标列名
	 * 
	 * @return 目标列名
	 */
	String getToName();

	/**
	 * 设置目标列名
	 * 
	 * @param toName
	 *            目标列名
	 */
	void setToName(String toName);

	/**
	 * 获取目标列数据类型
	 * 
	 * @return 目标列数据类型
	 */
	String getToType();

	/**
	 * 设置目标列数据类型
	 * 
	 * @param toType
	 *            目标列数据类型
	 */
	void setToType(String toType);

	/**
	 * 获取策略
	 * 
	 * @return 策略
	 */
	String getStrategy();

	/**
	 * 设置策略。可选值：both/from/to，both表示来源列和目标列均创建，from表示仅创建原来列，to表示仅创建目标列，默认为both。
	 * 
	 * @param strategy
	 *            策略
	 */
	void setStrategy(String strategy);

	/**
	 * 获取自定义脚本。通常是需要进行函数转换时使用
	 * 
	 * @return 自定义脚本
	 */
	String getScript();

	/**
	 * 设置自定义脚本。通常是需要进行函数转换时使用
	 * 
	 * @param script
	 *            自定义脚本
	 */
	void setScript(String script);

}
