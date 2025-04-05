package cn.tenmg.clink.config.model.bsh;

import java.io.Serializable;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;

/**
 * 变量配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Var implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7677686936463221422L;

	@XmlAttribute
	private String name;

	@XmlAttribute
	private String value;

	/**
	 * 获取变量名
	 * 
	 * @return 变量名
	 */
	public String getName() {
		return name;
	}

	/**
	 * 设置变量名
	 * 
	 * @param name
	 *            变量名
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * 获取变量值
	 * 
	 * @return 变量值
	 */
	public String getValue() {
		return value;
	}

	/**
	 * 设置变量值
	 * 
	 * @param value
	 *            变量值
	 */
	public void setValue(String value) {
		this.value = value;
	}

}
