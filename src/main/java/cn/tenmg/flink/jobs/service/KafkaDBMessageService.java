package cn.tenmg.flink.jobs.service;

import java.util.Arrays;
import java.util.Properties;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import cn.tenmg.flink.jobs.StreamService;
import cn.tenmg.flink.jobs.model.KafkaDBMessage;
import cn.tenmg.flink.jobs.model.Params;
import cn.tenmg.flink.jobs.serialization.KafkaDBMessageDeserializationSchema;

/**
 * 数据库表数据变更处理服务
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 */
public abstract class KafkaDBMessageService implements StreamService {

	/**
	 *
	 */
	private static final long serialVersionUID = 967105095937872674L;

	public static final String
	/**
	 * Kafka消费者group.id的前缀配置属性的键
	 */
	GROUP_ID_PREFIX_PROPERTY_KEY = "group.id.prefix",
			/**
			 * Kafka消费者偏移量配置属性的键
			 */
			STARTING_OFFSET_PROPERTY_KEY = "starting.offset";

	/**
	 * 获取订阅主题
	 * 
	 * @return 返回订阅主题
	 */
	protected abstract String getSubscribe();

	/**
	 * 获取Kafka配置属性
	 * 
	 * @return 返回Kafka配置属性
	 */
	protected abstract Properties getKafkaProperties();

	/**
	 * 获取过滤器
	 * 
	 * @return 如需过滤返回过滤器，无需过滤返回null
	 */
	protected abstract FilterFunction<KafkaDBMessage> getFilter();

	/**
	 * 根据运行参数获取批处理数据流
	 * 
	 * @param env
	 *            流运行环境
	 * @param params
	 *            运行参数
	 * @return 返回批处理数据流
	 */
	protected abstract DataStream<KafkaDBMessage> getBatchDataStream(StreamExecutionEnvironment env, Params params);

	/**
	 * 获取数据库操作的Kafka消息反序列化方案
	 * 
	 * @return 返回数据库操作的Kafka消息反序列化方案
	 */
	protected abstract KafkaDBMessageDeserializationSchema getKafkaDBMessageDeserializationSchema();

	/**
	 * 
	 * 
	 * @param env
	 *            流运行环境
	 * @param params
	 *            运行参数
	 * @param stream
	 *            数据库操作的kafka消息数据流
	 * @throws Exception
	 *             发生异常
	 */
	protected abstract void run(final StreamExecutionEnvironment env, Params params, DataStream<KafkaDBMessage> stream)
			throws Exception;

	@Override
	public void run(final StreamExecutionEnvironment env, Params params) throws Exception {
		DataStream<KafkaDBMessage> stream;
		if (RuntimeExecutionMode.BATCH.equals(params.getRuntimeMode())) {
			stream = getBatchDataStream(env, params);
		} else {
			Properties kafkaProperties = getKafkaProperties();
			kafkaProperties.setProperty("group.id",
					kafkaProperties.getProperty(GROUP_ID_PREFIX_PROPERTY_KEY, "flink-jobs").concat("_")
							.concat(params.getServiceName()));
			kafkaProperties.remove(GROUP_ID_PREFIX_PROPERTY_KEY);
			FlinkKafkaConsumerBase<KafkaDBMessage> flinkKafkaConsumer = new FlinkKafkaConsumer<KafkaDBMessage>(
					Arrays.asList(getSubscribe().split(",")), getKafkaDBMessageDeserializationSchema(),
					kafkaProperties);
			String startingOffset = kafkaProperties.getProperty(STARTING_OFFSET_PROPERTY_KEY);
			if ("earliest".equals(startingOffset)) {
				flinkKafkaConsumer.setStartFromEarliest();
			} else if ("groupOffsets".equals(startingOffset)) {
				flinkKafkaConsumer.setStartFromGroupOffsets();
			} else {
				flinkKafkaConsumer.setStartFromLatest();
			}
			kafkaProperties.remove(STARTING_OFFSET_PROPERTY_KEY);
			flinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);
			FilterFunction<KafkaDBMessage> filter = getFilter();
			if (filter == null) {
				stream = env.addSource(flinkKafkaConsumer);
			} else {
				stream = env.addSource(flinkKafkaConsumer).filter(filter);
			}
		}
		run(env, params, stream);
	}

}
