package cn.tenmg.flink.jobs.config.model;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlValue;

/**
 * JDBC操作配置模型
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Jdbc implements Operate, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6395425960958639543L;

	@XmlAttribute
	private String saveAs;

	@XmlAttribute
	private String when;

	@XmlAttribute
	private String dataSource;

	@XmlAttribute
	private String method;

	@XmlValue
	private String script;

	@Override
	public String getType() {
		return getClass().getSimpleName();
	}

	@Override
	public String getSaveAs() {
		return saveAs;
	}

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

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

}
