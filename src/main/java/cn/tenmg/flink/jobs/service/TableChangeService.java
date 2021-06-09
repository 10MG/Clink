package cn.tenmg.flink.jobs.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import cn.tenmg.flink.jobs.StreamService;
import cn.tenmg.flink.jobs.model.Params;
import cn.tenmg.flink.jobs.model.TableOperate;
import cn.tenmg.flink.jobs.serialization.TableOperateDeserializationSchema;
import cn.tenmg.flink.jobs.utils.TableUtils;

/**
 * 数据库表数据变更处理服务
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 */
public abstract class TableChangeService implements StreamService {

	/**
	 *
	 */
	private static final long serialVersionUID = 967105095937872674L;

	private static final Set<String> OPERATIONS = new HashSet<String>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = -786215989755170736L;

		{
			add("INSERT");
			add("UPDATE");
			add("DELETE");
			add("insert");
			add("update");
			add("delete");
		}
	};

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
	 * 获取特定的开始消费偏移量查找表
	 * 
	 * @return 返回特定的开始消费偏移量查找表
	 */
	protected abstract Map<KafkaTopicPartition, Long> getStartFromSpecificOffsets();

	/**
	 * 关联的表名集合。即哪些表数据变更（插入、更新、删除）后需要调用本类的process方法
	 * 
	 * @return 关联的表名数组
	 */
	protected abstract Set<String> getAssociatedTables();

	/**
	 * 根据运行参数获取批处理数据流
	 * 
	 * @param env
	 *            流运行环境
	 * @param params
	 *            运行参数
	 * @return 返回批处理数据流
	 */
	protected abstract DataStream<TableOperate> getBatchDataStream(StreamExecutionEnvironment env, Params params);

	/**
	 * 表数据变更处理方法。当任一关联的表数据发生变更时，调用本方法
	 * 
	 * @param fullTableName
	 *            表全名(大写)
	 * @param tableOperates
	 *            数据变更数据对象集
	 */
	protected abstract void process(String fullTableName, List<TableOperate> tableOperates);

	/**
	 * 获取最大尝试次数
	 * 
	 * @return 返回最大尝试次数
	 */
	protected abstract int getMaxTry();

	/**
	 * 获取消息后置处理器
	 * 
	 * @return 返回消息后置处理器
	 */
	protected abstract AfterTableChangeProcess getAfterTableChangeProcess();

	@Override
	public void run(final StreamExecutionEnvironment env, Params params) throws Exception {
		DataStream<TableOperate> stream;
		ProcessFunction<Tuple2<String, List<TableOperate>>, List<TableOperate>> processFunction;
		if (RuntimeExecutionMode.BATCH.equals(params.getRuntimeMode())) {
			stream = getBatchDataStream(env, params);
			processFunction = new BatchProcessFunction(this, getMaxTry());
		} else {
			Properties kafkaProperties = getKafkaProperties();
			kafkaProperties.setProperty("group.id", kafkaProperties.getProperty("group.id.prefix", "flink-jobs")
					.concat("_").concat(params.getServiceName()));
			kafkaProperties.remove("group.id.prefix");
			FlinkKafkaConsumerBase<TableOperate> flinkKafkaConsumer = new FlinkKafkaConsumer011<TableOperate>(
					Arrays.asList(getSubscribe().split(",")), new TableOperateDeserializationSchema(), kafkaProperties);
			flinkKafkaConsumer.setStartFromSpecificOffsets(getStartFromSpecificOffsets());
			stream = env.addSource(flinkKafkaConsumer).filter(new TableOperateFilter(getAssociatedTables()));
			processFunction = new StreamProcessFunction(this, getMaxTry(), getAfterTableChangeProcess());
		}
		SingleOutputStreamOperator<Tuple2<String, List<TableOperate>>> operator = stream
				.keyBy(value -> TableUtils.fullTableName(value.getDatabase(), value.getTable()).toUpperCase())
				.window(TumblingProcessingTimeWindows.of(Time.milliseconds(500)))
				.aggregate(new AggregateTableOperate());
		operator.process(processFunction);
	}

	protected static Consumer<Long, String> patitionsGetterConsumer(Properties properties) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.get("bootstrap.servers"));
		props.put(ConsumerConfig.GROUP_ID_CONFIG,
				properties.getProperty("group.id.prefix", "flink-jobs").concat("_partition_getter"));
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		return new KafkaConsumer<Long, String>(props);
	}

	// 流式处理
	public static class StreamProcessFunction
			extends ProcessFunction<Tuple2<String, List<TableOperate>>, List<TableOperate>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -8218678158529587523L;

		private final TableChangeService tableChangeHandler;

		private final int maxTry;

		private final AfterTableChangeProcess afterTableChangeProcess;

		public StreamProcessFunction(TableChangeService tableChangeHandler, int maxTry,
				AfterTableChangeProcess afterTableChangeProcess) {
			super();
			this.tableChangeHandler = tableChangeHandler;
			this.maxTry = maxTry;
			this.afterTableChangeProcess = afterTableChangeProcess;
		}

		@Override
		public void processElement(Tuple2<String, List<TableOperate>> value,
				ProcessFunction<Tuple2<String, List<TableOperate>>, List<TableOperate>>.Context ctx,
				Collector<List<TableOperate>> arg2) throws Exception {
			String fullTableName = value.f0, serviceName = tableChangeHandler.getClass().getSimpleName();
			List<TableOperate> tableOperates = value.f1;

			boolean success = false;
			for (int i = 0; i < maxTry; i++) {
				String msg = null;
				final Date startTime = Calendar.getInstance().getTime();
				try {
					tableChangeHandler.process(fullTableName, tableOperates);
					success = true;
				} catch (Exception e) {
					e.printStackTrace();
					msg = e.getMessage();
				}
				afterTableChangeProcess.log(tableOperates, serviceName, startTime, Calendar.getInstance().getTime(),
						success, msg);
				if (success) {
					afterTableChangeProcess.saveStartingOffsets(tableOperates.get(tableOperates.size() - 1),
							serviceName);
					break;
				}
			}
		}
	}

	// 批处理
	public static class BatchProcessFunction
			extends ProcessFunction<Tuple2<String, List<TableOperate>>, List<TableOperate>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -8995755672685262056L;

		private final TableChangeService tableChangeHandler;

		private final int maxTry;

		public BatchProcessFunction(TableChangeService tableChangeHandler, int maxTry) {
			super();
			this.tableChangeHandler = tableChangeHandler;
			this.maxTry = maxTry;
		}

		@Override
		public void processElement(Tuple2<String, List<TableOperate>> value,
				ProcessFunction<Tuple2<String, List<TableOperate>>, List<TableOperate>>.Context ctx,
				Collector<List<TableOperate>> arg2) throws Exception {
			String fullTableName = value.f0;
			List<TableOperate> tableOperates = value.f1;
			for (int i = 0; i < maxTry; i++) {
				try {
					tableChangeHandler.process(fullTableName, tableOperates);
					break;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	public static class TableOperateFilter implements FilterFunction<TableOperate> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -6146916944202432219L;

		private final Set<String> retainedTables;

		public TableOperateFilter(Set<String> retainedTables) {
			super();
			this.retainedTables = retainedTables;
		}

		@Override
		public boolean filter(TableOperate value) throws Exception {
			// 仅保留对特定表数据增删改的消息，其他均过滤掉
			if (retainedTables.contains(TableUtils.fullTableName(value.getDatabase(), value.getTable()).toUpperCase())
					&& OPERATIONS.contains(value.getType())) {
				return true;
			}
			return false;
		}

	}

	public static class AggregateTableOperate implements
			AggregateFunction<TableOperate, Tuple2<String, List<TableOperate>>, Tuple2<String, List<TableOperate>>> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 2762779066918191685L;

		@Override
		public Tuple2<String, List<TableOperate>> createAccumulator() {
			return new Tuple2<String, List<TableOperate>>();
		}

		@Override
		public Tuple2<String, List<TableOperate>> add(TableOperate value,
				Tuple2<String, List<TableOperate>> accumulator) {
			if (accumulator.f0 == null) {
				accumulator.f0 = TableUtils.fullTableName(value.getDatabase(), value.getTable()).toUpperCase();
			}
			List<TableOperate> list = accumulator.f1;
			if (list == null) {
				list = accumulator.f1 = new ArrayList<TableOperate>();
			}
			list.add(value);
			return accumulator;
		}

		@Override
		public Tuple2<String, List<TableOperate>> getResult(Tuple2<String, List<TableOperate>> accumulator) {
			return accumulator;
		}

		@Override
		public Tuple2<String, List<TableOperate>> merge(Tuple2<String, List<TableOperate>> a,
				Tuple2<String, List<TableOperate>> b) {
			a.f1.addAll(b.f1);
			return a;
		}
	}

}
