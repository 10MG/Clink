package cn.tenmg.flink.jobs.serialization;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import cn.tenmg.flink.jobs.model.KafkaDBMessage;
import cn.tenmg.flink.jobs.model.KafkaDBMessage.Operate;

/**
 * Debezium生成的数据库操作的Kafka消息反序列化方案
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.0.1
 */
public class DebeziumKafkaDBMessageDeserializationSchema extends KafkaDBMessageDeserializationSchema {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3846887220123950986L;

	private static final DebeziumKafkaDBMessageDeserializationSchema INSTANCE = new DebeziumKafkaDBMessageDeserializationSchema();

	private DebeziumKafkaDBMessageDeserializationSchema() {
		super();
	}

	public static DebeziumKafkaDBMessageDeserializationSchema getInstance() {
		return INSTANCE;
	}

	@Override
	void loadData(KafkaDBMessage kafkaDBMessage, ConsumerRecord<byte[], byte[]> record) throws Exception {
		String value = new String(record.value(), "UTF-8");
		JSONObject jo = JSON.parseObject(value);
		kafkaDBMessage.setAfter(jo.getString("after"));
		kafkaDBMessage.setBefore(jo.getString("before"));
		JSONObject source = jo.getJSONObject("source");
		kafkaDBMessage.setDatabase(source.getString("db"));
		String op = jo.getString("op");
		if (op == null) {
			kafkaDBMessage.setOperate(Operate.DDL);
		} else if (op.equalsIgnoreCase("c")) {
			kafkaDBMessage.setOperate(Operate.INSERT);
		} else if (op.equalsIgnoreCase("u")) {
			kafkaDBMessage.setOperate(Operate.UPDATE);
		} else if (op.equalsIgnoreCase("d")) {
			kafkaDBMessage.setOperate(Operate.DELETE);
		}
		kafkaDBMessage.setTable(source.getString("table"));
	}

}
