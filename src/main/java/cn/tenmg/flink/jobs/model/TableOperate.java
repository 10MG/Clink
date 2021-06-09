package cn.tenmg.flink.jobs.model;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.List;

import com.alibaba.fastjson.JSON;

/**
 * Canal生成的数据库操作的kafka消息实体类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 */
public class TableOperate implements Serializable {

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

	private String type;

	private String data;

	private List<String> pkNames;

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

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public List<String> getPkNames() {
		return pkNames;
	}

	public void setPkNames(List<String> pkNames) {
		this.pkNames = pkNames;
	}

	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}

}
