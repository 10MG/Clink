package cn.tenmg.flink.jobs.config.model;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlValue;

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
	private String catalog;

	@XmlValue
	private String script;

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
