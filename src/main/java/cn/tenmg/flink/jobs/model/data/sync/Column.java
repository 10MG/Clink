package cn.tenmg.flink.jobs.model.data.sync;

import java.io.Serializable;

/**
 * 数据列
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public class Column implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6593610552181924821L;

	private String fromName;

	private String fromType;

	private String toName;

	private String toType;

	private String script;

	private String strategy;

	/**
	 * 获取来源列名
	 * 
	 * @return 返回来源列名
	 */
	public String getFromName() {
		return fromName;
	}

	/**
	 * 设置来源列名
	 * 
	 * @param fromName
	 *            来源列名
	 */
	public void setFromName(String fromName) {
		this.fromName = fromName;
	}

	/**
	 * 获取目标列名
	 * 
	 * @return 返回目标列名
	 */
	public String getToName() {
		return toName;
	}

	/**
	 * 设置目标列名
	 * 
	 * @param toName
	 *            目标列名
	 */
	public void setToName(String toName) {
		this.toName = toName;
	}

	/**
	 * 获取来源列数据类型
	 * 
	 * @return 返回来源列数据类型
	 */
	public String getFromType() {
		return fromType;
	}

	/**
	 * 设置来源列数据类型
	 * 
	 * @param fromType
	 *            来源列数据类型
	 */
	public void setFromType(String fromType) {
		this.fromType = fromType;
	}

	/**
	 * 获取目标列数据类型
	 * 
	 * @return 返回目标列数据类型
	 */
	public String getToType() {
		return toType;
	}

	/**
	 * 设置目标列数据类型
	 * 
	 * @param toType
	 *            目标列数据类型
	 */
	public void setToType(String toType) {
		this.toType = toType;
	}

	/**
	 * 获取目标数据转换脚本
	 * 
	 * @return 返回目标数据转换脚本
	 */
	public String getScript() {
		return script;
	}

	/**
	 * 设置目标数据转换脚本
	 * 
	 * @param script
	 *            目标数据转换脚本
	 */
	public void setScript(String script) {
		this.script = script;
	}

	/**
	 * 获取策略
	 * 
	 * @return 策略
	 */
	public String getStrategy() {
		return strategy;
	}

	/**
	 * 设置策略。可选值：both/from/to，分别表示来源列和目标列均创建，from表示仅创建原来列，to表示仅创建目标列。
	 * 
	 * @param strategy
	 *            策略
	 */
	public void setStrategy(String strategy) {
		this.strategy = strategy;
	}

}
