package cn.tenmg.clink.config.model;

import java.io.Serializable;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlValue;

/**
 * 运行选项配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Option implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3776190871905065625L;

	@XmlAttribute
	private String key;

	@XmlValue
	private String value;

	public Option() {
		super();
	}

	public Option(String key) {
		super();
		this.key = key;
	}

	public Option(String key, String value) {
		super();
		this.key = key;
		this.value = value;
	}

	/**
	 * 获取选项键
	 * 
	 * @return 选项键
	 */
	public String getKey() {
		return key;
	}

	/**
	 * 设置选项键。如果键值以“-”开头，则不会添加默认前缀，否则会自动添加默认前缀keyPrefix
	 * 
	 * @param key
	 *            选项键
	 */
	public void setKey(String key) {
		this.key = key;
	}

	/**
	 * 获取选项值
	 * 
	 * @return 选项值
	 */
	public String getValue() {
		return value;
	}

	/**
	 * 设置选项值
	 * 
	 * @param value
	 *            选项值
	 */
	public void setValue(String value) {
		this.value = value;
	}

}
