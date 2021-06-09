package cn.tenmg.flink.jobs.serialization;

import java.sql.Timestamp;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.alibaba.fastjson.JSON;

import cn.tenmg.flink.jobs.model.TableOperate;

/**
 * Canal生成的数据库操作的Kafka消息反序列化方案
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 */
public class TableOperateDeserializationSchema implements KafkaDeserializationSchema<TableOperate> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 885554116570744907L;

	@Override
	public TypeInformation<TableOperate> getProducedType() {
		return TypeInformation.of(TableOperate.class);
	}

	@Override
	public boolean isEndOfStream(TableOperate nextElement) {
		return false;
	}

	@Override
	public TableOperate deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		String value = new String(record.value(), "UTF-8");
		TableOperate tableOperate = JSON.parseObject(value, TableOperate.class);
		tableOperate.setTopic(record.topic());
		tableOperate.setPartition(record.partition());
		tableOperate.setOffset(record.offset());
		tableOperate.setTimestamp(new Timestamp(record.timestamp()));
		tableOperate.setTimestampType(record.timestampType().id);
		return tableOperate;
	}

}
