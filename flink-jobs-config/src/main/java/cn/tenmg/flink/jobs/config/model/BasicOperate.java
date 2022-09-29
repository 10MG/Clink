package cn.tenmg.flink.jobs.config.model;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 * 基本操作配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class BasicOperate implements Operate, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1509738902071845448L;

	@XmlAttribute
	private String saveAs;

	@XmlAttribute
	private String when;

	/**
	 * 获取操作类型
	 * 
	 * @return 操作类型
	 */
	@Override
	public String getType() {
		return getClass().getSimpleName();
	};

	/**
	 * 获取处理结果另存为变量名
	 * 
	 * @return 处理结果另存为变量名
	 */
	@Override
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
	@Override
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
