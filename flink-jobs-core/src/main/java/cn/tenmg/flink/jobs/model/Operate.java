package cn.tenmg.flink.jobs.model;

import java.io.Serializable;

/**
 * 操作配置虚基类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public class Operate implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1680848104272110347L;

	private String saveAs;

	private String when;

	/**
	 * 获取操作类型
	 * 
	 * @return 操作类型
	 */
	public String getType() {
		return getClass().getSimpleName();
	};

	/**
	 * 获取处理结果另存为变量名
	 * 
	 * @return 处理结果另存为变量名
	 */
	public String getSaveAs() {
		return saveAs;
	}

	/**
	 * 设置处理结果另存为变量名
	 * 
	 * @param saveAs
	 *            处理结果另存为变量名
	 */
	public void setSaveAs(String saveAs) {
		this.saveAs = saveAs;
	}

	/**
	 * 获取处理条件
	 * 
	 * @return 处理条件
	 */
	public String getWhen() {
		return when;
	}

	/**
	 * 设置处理条件
	 * 
	 * @param when
	 *            处理条件
	 */
	public void setWhen(String when) {
		this.when = when;
	}

}
