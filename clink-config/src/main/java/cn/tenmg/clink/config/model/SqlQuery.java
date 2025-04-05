package cn.tenmg.clink.config.model;

import java.io.Serializable;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlValue;

/**
 * Flink SQL的sqlQuery操作配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class SqlQuery implements Operate, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6902349255064231962L;

	@XmlAttribute
	private String saveAs;

	@XmlAttribute
	private String when;

	@XmlAttribute
	private String catalog;

	@XmlValue
	private String script;

	/**
	 * 获取操作类型
	 * 
	 * @return 操作类型
	 */
	@Override
	public String getType() {
		return getClass().getSimpleName();
	};

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

	@Override
	public String getWhen() {
		return when;
	}

	public void setWhen(String when) {
		this.when = when;
	}

	/**
	 * 获取使用的目录
	 * 
	 * @return 使用的目录
	 */
	public String getCatalog() {
		return catalog;
	}

	/**
	 * 设置使用的目录
	 * 
	 * @param catalog
	 *            使用的目录
	 */
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}

	/**
	 * 获取SQL脚本
	 * 
	 * @return SQL脚本
	 */
	public String getScript() {
		return script;
	}

	/**
	 * 设置SQL脚本
	 * 
	 * @param script
	 *            SQL脚本
	 */
	public void setScript(String script) {
		this.script = script;
	}
}
