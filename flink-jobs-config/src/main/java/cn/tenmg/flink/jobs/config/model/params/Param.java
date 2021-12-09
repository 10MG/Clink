package cn.tenmg.flink.jobs.config.model.params;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlValue;

/**
 * 参数配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Param {

	@XmlAttribute
	private String name;

	@XmlValue
	private String value;

	/**
	 * 获取参数名
	 * 
	 * @return 参数名
	 */
	public String getName() {
		return name;
	}

	/**
	 * 设置参数名
	 * 
	 * @param name
	 *            参数名
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * 获取参数值
	 * 
	 * @return 参数值
	 */
	public String getValue() {
		return value;
	}

	/**
	 * 设置参数值
	 * 
	 * @param value
	 *            参数值
	 */
	public void setValue(String value) {
		this.value = value;
	}

}
