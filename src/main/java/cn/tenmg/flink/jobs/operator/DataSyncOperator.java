package cn.tenmg.flink.jobs.operator;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.model.DataSync;
import cn.tenmg.flink.jobs.model.data.sync.Column;
import cn.tenmg.flink.jobs.utils.ConfigurationUtils;
import cn.tenmg.flink.jobs.utils.SQLUtils;

/**
 * 数据同步操作执行器
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public class DataSyncOperator extends AbstractOperator<DataSync> {

	private static final String SMART_KEY = "data.sync.smart", FALSE = "false";

	@Override
	Object execute(StreamExecutionEnvironment env, DataSync dataSync, Map<String, Object> params) throws Exception {
		String from = dataSync.getFrom(), to = dataSync.getTo(), table = dataSync.getTable(),
				primaryKey = dataSync.getPrimaryKey();
		if (StringUtils.isBlank(from) || StringUtils.isBlank(to) || StringUtils.isBlank(table)) {
			throw new IllegalArgumentException("The property from, to or table of DataSync cannot be blank.");
		}
		Map<String, String> fromDataSource = FlinkJobsContext.getDatasource(from),
				toDataSource = FlinkJobsContext.getDatasource(to);
		List<Column> columns = dataSync.getColumns();
		if (FALSE.equals(FlinkJobsContext.getProperty(SMART_KEY))) {

		} else {

		}
		return null;
	}

	private static String formCreateTableSQL(Map<String, String> dataSource, String tableName, List<Column> columns,
			String primaryKey, String fromConfig) throws IOException {
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("CREATE TABLE ").append(tableName).append("(");
		Column column = columns.get(0);
		sqlBuffer.append(column.getFromName()).append(DSLUtils.BLANK_SPACE).append(column.getFromType());
		for (int i = 1, size = columns.size(); i < size; i++) {
			column = columns.get(i);
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(column.getFromName())
					.append(DSLUtils.BLANK_SPACE).append(column.getFromType());
		}
		if (StringUtils.isNotBlank(primaryKey)) {
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append("PRIMARY KEY (").append(primaryKey)
					.append(") NOT ENFORCED");
		}
		sqlBuffer.append(") ").append("WITH (");
		if (StringUtils.isBlank(fromConfig)) {
			SQLUtils.appendDataSource(sqlBuffer, dataSource);
		} else {
			LinkedHashMap<String, String> actualDataSource = new LinkedHashMap<String, String>();
			actualDataSource.putAll(dataSource);
			for (Iterator<String> it = ConfigurationUtils.load(fromConfig).keySet().iterator(); it.hasNext();) {
				actualDataSource.remove(it.next());
			}
			SQLUtils.appendDataSource(sqlBuffer, actualDataSource);
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(fromConfig);
		}
		sqlBuffer.append(")");
		return sqlBuffer.toString();
	}

	public static void main(String[] args) throws IOException {
		Map<String, String> dataSource = new HashMap<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 7684549968969231800L;

			{
				put("connector", "kafka");
				put("properties.bootstrap.servers", "192.168.81.103:9092,192.168.81.104:9092,192.168.81.105:9092");
				put("properties.group.id", "sinochem-flink-jobs");
				put("scan.startup.mode", "earliest-offset");
				put("format", "debezium-json");
				put("debezium-json.schema-include", "true");
			}
		};
		List<Column> columns = new ArrayList<Column>();
		Column column = new Column();
		column.setFromName("order_id");
		column.setFromType("STRING");
		columns.add(column);
		System.out.println(formCreateTableSQL(dataSource, "kafka_order_info", columns, "order_id",
				"'topic' = 'kaorder.kaorder.order_detail', 'properties.group.id' = 'flink-jobs_kaorder_order_detail'"));
		System.out.println(ConfigurationUtils.load(
				"'topic'='kaorder.kaorder.order_detail', 'properties.group.id'='flink-jobs_kaorder_order_detail'"));

	}

}
