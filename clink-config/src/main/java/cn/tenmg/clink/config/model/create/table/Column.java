package cn.tenmg.clink.config.model.create.table;

import java.io.Serializable;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.XmlValue;

/**
 * 数据列
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.3.0
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "create-table>column")
public class Column implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7744054869336052412L;

	/**
	 * 列名
	 */
	@XmlAttribute
	private String name;

	/**
	 * 数据类型
	 */
	@XmlValue
	private String type;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

}
