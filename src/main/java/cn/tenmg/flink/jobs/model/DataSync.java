package cn.tenmg.flink.jobs.model;

import java.util.List;

import cn.tenmg.flink.jobs.model.data.sync.Column;

/**
 * 数据同步配置
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public class DataSync extends Operate {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7644247317720269610L;

	private String from;

	private String topic;

	private String fromConfig;

	private String to;

	private String toConfig;

	private String table;

	private String primaryKey;

	private String timestampColumnName;

	private Boolean smart;

	private List<Column> columns;

	/**
	 * 获取来源数据源名称
	 * 
	 * @return 返回来源数据源名称
	 */
	public String getFrom() {
		return from;
	}

	/**
	 * 设置来源数据源名称
	 * 
	 * @param from
	 *            来源数据源名称
	 */
	public void setFrom(String from) {
		this.from = from;
	}

	/**
	 * 获取主题
	 * 
	 * @return 返回主题
	 */
	public String getTopic() {
		return topic;
	}

	/**
	 * 设置主题
	 * 
	 * @param topic
	 *            主题
	 */
	public void setTopic(String topic) {
		this.topic = topic;
	}

	/**
	 * 获取来源配置
	 * 
	 * @return 返回来源配置
	 */
	public String getFromConfig() {
		return fromConfig;
	}

	/**
	 * 设置来源配置
	 * 
	 * @param fromConfig
	 *            来源配置
	 */
	public void setFromConfig(String fromConfig) {
		this.fromConfig = fromConfig;
	}

	/**
	 * 获取目标数据源名称
	 * 
	 * @return 返回目标数据源名称
	 */
	public String getTo() {
		return to;
	}

	/**
	 * 设置目标数据源名称
	 * 
	 * @param to
	 *            目标数据源名称
	 */
	public void setTo(String to) {
		this.to = to;
	}

	/**
	 * 获取目标配置
	 * 
	 * @return 目标配置
	 */
	public String getToConfig() {
		return toConfig;
	}

	/**
	 * 设置目标配置
	 * 
	 * @param toConfig
	 *            目标配置
	 */
	public void setToConfig(String toConfig) {
		this.toConfig = toConfig;
	}

	/**
	 * 获取同步数据表名
	 * 
	 * @return 返回同步数据表名
	 */
	public String getTable() {
		return table;
	}

	/**
	 * 设置同步数据表名
	 * 
	 * @param table
	 *            同步数据表名
	 */
	public void setTable(String table) {
		this.table = table;
	}

	/**
	 * 获取主键
	 * 
	 * @return 返回主键
	 */
	public String getPrimaryKey() {
		return primaryKey;
	}

	/**
	 * 设置主键
	 * 
	 * @param primaryKey
	 *            主键。如为复合主键，多个字段名之间使用“,”分隔
	 */
	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	/**
	 * 获取智能模式状态
	 * 
	 * @return 返回智能模式状态
	 */
	public Boolean getSmart() {
		return smart;
	}

	/**
	 * 设置智能模式状态。true表示开启智能模式，即自动查询列名和数据类型信息，false则表示仅使用指定的列执行数据同步，不设置表示使用配置文件的配置，如果配置文件未指定则默认为true。
	 * 
	 * @param smart
	 *            智能模式状态
	 */
	public void setSmart(Boolean smart) {
		this.smart = smart;
	}

	/**
	 * 获取时间戳字段名
	 * 
	 * @return 时间戳字段名
	 */
	public String getTimestampColumnName() {
		return timestampColumnName;
	}

	/**
	 * 设置时间戳字段名。设置这个值后，会使用这个字段名创建源表和目标表，并在数据同步时写入这个字段值。
	 * 
	 * @param timestampColumnName
	 *            时间戳字段名
	 */
	public void setTimestampColumnName(String timestampColumnName) {
		this.timestampColumnName = timestampColumnName;
	}

	/**
	 * 获取同步数据列
	 * 
	 * @return 返回同步数据列
	 */
	public List<Column> getColumns() {
		return columns;
	}

	/**
	 * 设置同步数据列
	 * 
	 * @param columns
	 *            同步数据列
	 */
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

}
