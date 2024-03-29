package cn.tenmg.clink.config.model.data.sync;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlType;
import jakarta.xml.bind.annotation.XmlValue;

/**
 * 同步数据复杂列
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.4.0
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "data-sync>column")
public class SimpleColumn implements Column {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2629483024607637740L;
	/**
	 * 来源列名
	 */
	@XmlAttribute
	private String fromName;
	/**
	 * 来源列数据类型。如果缺省，则如果开启智能模式会自动获取目标数据类型作为来源数据类型，如果关闭智能模式则必填
	 */
	@XmlAttribute
	private String fromType;
	/**
	 * 目标列名。默认为来源列名
	 */
	@XmlAttribute
	private String toName;
	/**
	 * 目标列数据类型。如果缺省，则如果开启智能模式会自动获取，如果关闭智能模式则默认为来源列数据类型
	 */
	@XmlAttribute
	private String toType;
	/**
	 * 策略。可选值：both/from/to，both表示来源列和目标列均创建，from表示仅创建原来列，to表示仅创建目标列，默认为both。
	 */
	@XmlAttribute
	private String strategy;
	/**
	 * 自定义脚本。通常是需要进行函数转换时使用
	 */
	@XmlValue
	private String script;

	public String getFromName() {
		return fromName;
	}

	public void setFromName(String fromName) {
		this.fromName = fromName;
	}

	public String getFromType() {
		return fromType;
	}

	public void setFromType(String fromType) {
		this.fromType = fromType;
	}

	public String getToName() {
		return toName;
	}

	public void setToName(String toName) {
		this.toName = toName;
	}

	public String getToType() {
		return toType;
	}

	public void setToType(String toType) {
		this.toType = toType;
	}

	public String getStrategy() {
		return strategy;
	}

	public void setStrategy(String strategy) {
		this.strategy = strategy;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

}
