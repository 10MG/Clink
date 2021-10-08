package cn.tenmg.flink.jobs.model;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * 数据库操作的kafka消息实体类。已废弃，将在下一版本移除
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 * @since 1.0.1
 */
@Deprecated
public class KafkaDBMessage implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 471954818284607308L;

	private String topic;

	private int partition;

	private Timestamp timestamp;

	private int timestampType;

	private long offset;

	private String database;

	private String table;

	private Operate operate;

	private String before;

	private String after;

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public int getTimestampType() {
		return timestampType;
	}

	public void setTimestampType(int timestampType) {
		this.timestampType = timestampType;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public Operate getOperate() {
		return operate;
	}

	public void setOperate(Operate operate) {
		this.operate = operate;
	}

	public String getBefore() {
		return before;
	}

	public void setBefore(String before) {
		this.before = before;
	}

	public String getAfter() {
		return after;
	}

	public void setAfter(String after) {
		this.after = after;
	}

	public enum Operate {
		INSERT, UPDATE, DELETE, DDL
	}
}
