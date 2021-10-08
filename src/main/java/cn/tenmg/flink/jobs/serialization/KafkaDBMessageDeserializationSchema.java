package cn.tenmg.flink.jobs.serialization;

import java.sql.Timestamp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import cn.tenmg.flink.jobs.model.KafkaDBMessage;

/**
 * 数据库操作的Kafka消息反序列化方案。已废弃，将在下一版本移除
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.0.1
 */
@Deprecated
public abstract class KafkaDBMessageDeserializationSchema implements KafkaDeserializationSchema<KafkaDBMessage> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1380934886771584548L;

	@Override
	public TypeInformation<KafkaDBMessage> getProducedType() {
		return TypeInformation.of(KafkaDBMessage.class);
	}

	@Override
	public boolean isEndOfStream(KafkaDBMessage nextElement) {
		return false;
	}

	@Override
	public KafkaDBMessage deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		KafkaDBMessage kafkaDBMessage = new KafkaDBMessage();
		kafkaDBMessage.setTopic(record.topic());
		kafkaDBMessage.setPartition(record.partition());
		kafkaDBMessage.setOffset(record.offset());
		kafkaDBMessage.setTimestamp(new Timestamp(record.timestamp()));
		kafkaDBMessage.setTimestampType(record.timestampType().id);
		loadData(kafkaDBMessage, record);
		return kafkaDBMessage;
	}

	/**
	 * 加载Kafka消息数据
	 * 
	 * @param kafkaDBMessage
	 *            数据库操作的kafka消息实体类
	 * @param record
	 *            Kafka消费记录
	 * @throws Exception
	 *             发生异常
	 */
	abstract void loadData(KafkaDBMessage kafkaDBMessage, ConsumerRecord<byte[], byte[]> record) throws Exception;

}
