package cn.tenmg.flink.jobs.config.model.create.table;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlValue;

/**
 * 数据列
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.3.0
 */
@XmlAccessorType(XmlAccessType.FIELD)
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
