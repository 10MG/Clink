package cn.tenmg.flink.jobs.model.data.sync;

import java.io.Serializable;

/**
 * 数据列
 * 
 * @author 赵伟均 wjzhao@aliyun.com
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
	 * @param formName
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
	 * @param formName
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
	 * @param formType
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

}
