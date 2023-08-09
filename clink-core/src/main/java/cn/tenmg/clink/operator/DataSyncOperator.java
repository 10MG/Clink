package cn.tenmg.clink.operator;

import java.util.List;
import java.util.Map;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cn.tenmg.clink.exception.IllegalJobConfigException;
import cn.tenmg.clink.model.DataSync;
import cn.tenmg.clink.model.data.sync.Column;
import cn.tenmg.clink.operator.job.generator.MultiTablesDataSyncJobGenerator;
import cn.tenmg.clink.operator.job.generator.SingleTableDataSyncJobGenerator;
import cn.tenmg.dsl.utils.CollectionUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 数据同步操作执行器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public class DataSyncOperator extends AbstractOperator<DataSync> {

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
		return (table.contains(",") ? MultiTablesDataSyncJobGenerator.getInstance()
				: SingleTableDataSyncJobGenerator.getInstance()).generate(env, dataSync, params);
	}

}