package cn.tenmg.flink.jobs.serialization;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import cn.tenmg.flink.jobs.model.KafkaDBMessage;
import cn.tenmg.flink.jobs.model.KafkaDBMessage.Operate;

/**
 * Canal生成的数据库操作的Kafka消息反序列化方案。已废弃，将在下一版本移除
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.0.1
 */
@Deprecated
public class CanalKafkaDBMessageDeserializationSchema extends KafkaDBMessageDeserializationSchema {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1766084960256345794L;

	private static final CanalKafkaDBMessageDeserializationSchema INSTANCE = new CanalKafkaDBMessageDeserializationSchema();

	private CanalKafkaDBMessageDeserializationSchema() {
		super();
	}

	public static CanalKafkaDBMessageDeserializationSchema getInstance() {
		return INSTANCE;
	}

	@Override
	void loadData(KafkaDBMessage kafkaDBMessage, ConsumerRecord<byte[], byte[]> record) throws Exception {
		String value = new String(record.value(), "UTF-8");
		JSONObject jo = JSON.parseObject(value);
		kafkaDBMessage.setAfter(jo.getString("data"));
		kafkaDBMessage.setBefore(jo.getString("old"));
		kafkaDBMessage.setDatabase(jo.getString("database"));
		String type = jo.getString("type");
		if (type == null) {
			kafkaDBMessage.setOperate(Operate.DDL);
		} else if (type.equalsIgnoreCase("insert")) {
			kafkaDBMessage.setOperate(Operate.INSERT);
		} else if (type.equalsIgnoreCase("update")) {
			kafkaDBMessage.setOperate(Operate.UPDATE);
		} else if (type.equalsIgnoreCase("delete")) {
			kafkaDBMessage.setOperate(Operate.DELETE);
		}
		kafkaDBMessage.setTable(jo.getString("table"));
	}

}
