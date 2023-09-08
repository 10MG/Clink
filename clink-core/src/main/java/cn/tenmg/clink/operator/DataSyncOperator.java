package cn.tenmg.clink.operator;

import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.clink.exception.IllegalJobConfigException;
import cn.tenmg.clink.model.DataSync;
import cn.tenmg.clink.model.data.sync.Column;
import cn.tenmg.clink.operator.job.DataSyncJobGenerator;
import cn.tenmg.clink.operator.job.generator.MultiTablesDataSyncJobGenerator;
import cn.tenmg.clink.operator.job.generator.SingleTableDataSyncJobGenerator;
import cn.tenmg.clink.utils.ConfigurationUtils;
import cn.tenmg.clink.utils.DataSourceFilterUtils;
import cn.tenmg.clink.utils.SQLUtils;
import cn.tenmg.dsl.utils.CollectionUtils;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 数据同步操作执行器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public class DataSyncOperator extends AbstractOperator<DataSync> {

	private static final String CONVERT_DELETE_TO_UPDATE = "convert-delete-to-update";

	@Override
	public Object execute(StreamExecutionEnvironment env, DataSync dataSync, Map<String, Object> params)
			throws Exception {
		String from = dataSync.getFrom(), to = dataSync.getTo(), table = dataSync.getTable();
		if (StringUtils.isBlank(from) || StringUtils.isBlank(to) || StringUtils.isBlank(table)) {
			throw new IllegalJobConfigException("The property 'from', 'to' or 'table' cannot be blank.");
		}
		List<Column> columns = dataSync.getColumns();
		if (CollectionUtils.isNotEmpty(columns)) {
			for (int i = 0, size = columns.size(); i < size; i++) {
				if (StringUtils.isBlank(columns.get(i).getFromName())) {
					throw new IllegalJobConfigException(
							"The property 'fromName' cannot be blank at the column wich index is " + i);
				}
			}
		}
		Map<String, String> sourceDataSource = DataSourceFilterUtils.filter("source", ClinkContext.getDatasource(from)),
				sinkDataSource = DataSourceFilterUtils.filter("sink", ClinkContext.getDatasource(to));
		String fromConfig = dataSync.getFromConfig(), toConfig = dataSync.getToConfig();
		if (StringUtils.isNotBlank(fromConfig)) {
			sourceDataSource.putAll(ConfigurationUtils.load(SQLUtils.toSQL(DSLUtils.parse(fromConfig, params))));// 解析其中的参数并加载配置
		}
		if (StringUtils.isNotBlank(toConfig)) {
			sinkDataSource.putAll(ConfigurationUtils.load(SQLUtils.toSQL(DSLUtils.parse(toConfig, params))));// 解析其中的参数并加载配置
		}
		DataSyncJobGenerator dataSyncJobGenerator;
		if (table.contains(",") || "true".equals(sourceDataSource.get(CONVERT_DELETE_TO_UPDATE))) {
			dataSyncJobGenerator = MultiTablesDataSyncJobGenerator.getInstance();
		} else {
			sourceDataSource.remove(CONVERT_DELETE_TO_UPDATE);
			dataSyncJobGenerator = SingleTableDataSyncJobGenerator.getInstance();
		}
		return dataSyncJobGenerator.generate(env, dataSync, sourceDataSource, sinkDataSource, params);
	}

}