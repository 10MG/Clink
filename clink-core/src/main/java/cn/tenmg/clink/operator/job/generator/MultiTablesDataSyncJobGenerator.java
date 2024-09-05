package cn.tenmg.clink.operator.job.generator;

import java.util.Map;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import cn.tenmg.clink.exception.IllegalConfigurationException;
import cn.tenmg.clink.model.DataSync;
import cn.tenmg.clink.source.SourceFactory;
import cn.tenmg.clink.utils.SourceFactoryUtils;

/**
 * 多表数据同步作业生成器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class MultiTablesDataSyncJobGenerator extends FromSourceFactoryDataSyncJobGenerator {

	private static final MultiTablesDataSyncJobGenerator INSTANCE = new MultiTablesDataSyncJobGenerator();

	private MultiTablesDataSyncJobGenerator() {
	}

	public static MultiTablesDataSyncJobGenerator getInstance() {
		return INSTANCE;
	}

	@Override
	Object generate(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, DataSync dataSync,
			Map<String, String> sourceDataSource, Map<String, String> sinkDataSource, Map<String, Object> params)
			throws Exception {
		String connector = sourceDataSource.get("connector");
		SourceFactory<Source<Tuple2<String, Row>, ?, ?>> sourceFactory = SourceFactoryUtils.getSourceFactory(connector);
		if (sourceFactory == null) {
			throw new IllegalConfigurationException("Cannot find source factory for connector " + connector);
		}
		return generate(env, tableEnv, sourceFactory, dataSync, sourceDataSource, sinkDataSource, params);
	}

}
