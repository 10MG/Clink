package cn.tenmg.flink.jobs.config.model;

import java.io.Serializable;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

import cn.tenmg.flink.jobs.config.model.data.sync.Column;

/**
 * 数据同步
 * 
 * @author June wjzhao@aliyun.com cbb 2545095524@qq.com
 * 
 * @since 1.1.4
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class DataSync implements Operate, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7762957722633631338L;

	@XmlAttribute
	private String saveAs;

	/**
	 * 来源数据源名称
	 */
	@XmlAttribute
	private String from;
	/**
	 * Kafka主题。也可在from-config中配置`topic=xxx`
	 */
	@XmlAttribute
	private String topic;
	/**
	 * 目标数据源名称
	 */
	@XmlAttribute
	private String to;
	/**
	 * 同步数据表名
	 */
	@XmlAttribute
	private String table;
	/**
	 * 主键，多个列名以“,”分隔。当开启智能模式时，会自动获取主键信息。
	 */
	@XmlAttribute
	private String primaryKey;

	/**
	 * 时间戳字段名，多个字段名使用“,”分隔。设置这个值后，会使用这些字段名创建源表和目标表，并在数据同步时写入这些字段值。
	 */
	@XmlAttribute
	private String timestamp;

	/**
	 * 智能模式状态。`true`表示开启智能模式，即自动查询列名和数据类型信息，`false`则表示仅使用指定的列执行数据同步，不设置表示使用配置文件的配置，如果配置文件未指定则默认为`true`。
	 */
	@XmlAttribute
	private Boolean smart;

	/**
	 * 来源配置。例如：`properties.group.id=flink-jobs`
	 */
	@XmlElement(name = "from-config", namespace = FlinkJobs.NAMESPACE)
	private String fromConfig;

	/**
	 * 目标配置。例如：`sink.buffer-flush.max-rows = 0`
	 */
	@XmlElement(name = "to-config", namespace = FlinkJobs.NAMESPACE)
	private String toConfig;

	/**
	 * 同步数据列
	 */
	@XmlElement(name = "column", namespace = FlinkJobs.NAMESPACE)
	private List<Column> columns;

	@Override
	public String getType() {
		return getClass().getSimpleName();
	}

	public String getSaveAs() {
		return saveAs;
	}

	public void setSaveAs(String saveAs) {
		this.saveAs = saveAs;
	}

	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getTo() {
		return to;
	}

	public void setTo(String to) {
		this.to = to;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public Boolean getSmart() {
		return smart;
	}

	public void setSmart(Boolean smart) {
		this.smart = smart;
	}

	public String getFromConfig() {
		return fromConfig;
	}

	public void setFromConfig(String fromConfig) {
		this.fromConfig = fromConfig;
	}

	public String getToConfig() {
		return toConfig;
	}

	public void setToConfig(String toConfig) {
		this.toConfig = toConfig;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

}
