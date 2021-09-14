package cn.tenmg.flink.jobs.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.model.DataSync;
import cn.tenmg.flink.jobs.model.data.sync.Column;
import cn.tenmg.flink.jobs.utils.ConfigurationUtils;
import cn.tenmg.flink.jobs.utils.MapUtils;
import cn.tenmg.flink.jobs.utils.SQLUtils;

/**
 * 数据同步操作执行器
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public class DataSyncOperator extends AbstractOperator<DataSync> {

	private static final String SMART_KEY = "data.sync.smart", FORM_TABLE_PREFIX_KEY = "data.sync.form_table_prefix",
			TOPIC_KEY = "topic";

	@Override
	Object execute(StreamExecutionEnvironment env, DataSync dataSync, Map<String, Object> params) throws Exception {
		String from = dataSync.getFrom(), to = dataSync.getTo(), table = dataSync.getTable();
		if (StringUtils.isBlank(from) || StringUtils.isBlank(to) || StringUtils.isBlank(table)) {
			throw new IllegalArgumentException("The property from, to or table of DataSync cannot be blank.");
		}
		StreamTableEnvironment tableEnv = FlinkJobsContext.getOrCreateStreamTableEnvironment(env);
		String primaryKey = dataSync.getPrimaryKey(), topic = dataSync.getTopic(),
				currentCatalog = tableEnv.getCurrentCatalog(),
				defaultCatalog = FlinkJobsContext.getDefaultCatalog(tableEnv),
				fromTable = FlinkJobsContext.get(FORM_TABLE_PREFIX_KEY) + table;
		if (!defaultCatalog.equals(currentCatalog)) {
			tableEnv.useCatalog(defaultCatalog);
		}
		Map<String, String> fromDataSource = FlinkJobsContext.getDatasource(from),
				toDataSource = FlinkJobsContext.getDatasource(to);
		List<Column> columns = dataSync.getColumns();
		Boolean smart = dataSync.getSmart();
		if (smart == null) {
			smart = Boolean.valueOf(FlinkJobsContext.getProperty(SMART_KEY));
		}
		if (Boolean.TRUE.equals(smart)) {// 智能模式，自动查询列名、数据类型
			
		}
		tableEnv.executeSql(
				fromCreateTableSQL(fromDataSource, topic, fromTable, columns, primaryKey, dataSync.getFromConfig()));
		tableEnv.executeSql(toCreateTableSQL(toDataSource, table, columns, primaryKey, dataSync.getToConfig()));
		return tableEnv.executeSql(insertSQL(table, fromTable, columns));
	}

	private static String fromCreateTableSQL(Map<String, String> dataSource, String topic, String fromTable,
			List<Column> columns, String primaryKey, String fromConfig) throws IOException {
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("CREATE TABLE ").append(fromTable).append("(");
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
			if (topic == null) {
				SQLUtils.appendDataSource(sqlBuffer, dataSource);
			} else {
				Map<String, String> actualDataSource = MapUtils.newHashMap(dataSource);
				actualDataSource.put(TOPIC_KEY, topic);
				SQLUtils.appendDataSource(sqlBuffer, actualDataSource);
			}
		} else {
			Map<String, String> actualDataSource = MapUtils.newHashMap(dataSource),
					config = ConfigurationUtils.load(fromConfig);
			MapUtils.removeAll(actualDataSource, config.keySet());
			if (topic != null && !config.containsKey(TOPIC_KEY)) {
				actualDataSource.put(TOPIC_KEY, topic);
			}
			SQLUtils.appendDataSource(sqlBuffer, actualDataSource);
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(fromConfig);
		}
		sqlBuffer.append(")");
		return sqlBuffer.toString();
	}

	private static String toCreateTableSQL(Map<String, String> dataSource, String table, List<Column> columns,
			String primaryKey, String toConfig) throws IOException {
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("CREATE TABLE ").append(table).append("(");
		Column column = columns.get(0);
		String toName = column.getToName(), toType = column.getToType();
		sqlBuffer.append(toName == null ? column.getFromName() : toName).append(DSLUtils.BLANK_SPACE)
				.append(toType == null ? column.getFromType() : toType);
		for (int i = 1, size = columns.size(); i < size; i++) {
			column = columns.get(i);
			toName = column.getToName();
			toType = column.getToType();
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE)
					.append(toName == null ? column.getFromName() : toName).append(DSLUtils.BLANK_SPACE)
					.append(toType == null ? column.getFromType() : toType);
		}
		if (StringUtils.isNotBlank(primaryKey)) {
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append("PRIMARY KEY (").append(primaryKey)
					.append(") NOT ENFORCED");
		}

		sqlBuffer.append(") ").append("WITH (");
		if (StringUtils.isBlank(toConfig)) {
			SQLUtils.appendDataSource(sqlBuffer, dataSource);
		} else {
			Map<String, String> actualDataSource = MapUtils.newHashMap(dataSource),
					config = ConfigurationUtils.load(toConfig);
			MapUtils.removeAll(actualDataSource, config.keySet());
			SQLUtils.appendDataSource(sqlBuffer, actualDataSource);
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(toConfig);
		}
		sqlBuffer.append(")");
		return sqlBuffer.toString();
	}

	private static String insertSQL(String table, String fromTable, List<Column> columns) {
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("INSERT INTO ").append(table).append(DSLUtils.BLANK_SPACE).append("(");

		Column column = columns.get(0);
		String toName = column.getToName();
		sqlBuffer.append(toName == null ? column.getFromName() : toName);
		for (int i = 1, size = columns.size(); i < size; i++) {
			column = columns.get(i);
			toName = column.getToName();
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE)
					.append(toName == null ? column.getFromName() : toName);
		}

		sqlBuffer.append(") SELECT ");
		column = columns.get(0);
		String script = column.getScript();
		sqlBuffer.append(script == null ? column.getFromName() : script);
		for (int i = 1, size = columns.size(); i < size; i++) {
			column = columns.get(i);
			script = column.getScript();
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE)
					.append(script == null ? column.getFromName() : script);
		}

		sqlBuffer.append(" FROM ").append(fromTable);
		return sqlBuffer.toString();

	}

	public static void main(String[] args) throws IOException {
		Map<String, String> kafka = new HashMap<String, String>() {
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
		}, bidb = new HashMap<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 7684549968969231800L;

			{
				put("connector", "jdbc");
				put("driver", "org.postgresql.Driver");
				put("url", "jdbc:postgresql://192.168.1.104:5432/bidb");
				put("username", "your_name");
				put("password", "your_password");
			}
		};
		List<Column> columns = new ArrayList<Column>();
		Column column = new Column();
		column.setFromName("order_id");
		column.setFromType("STRING");
		columns.add(column);
		System.out.println(fromCreateTableSQL(kafka, "kaorder.kaorder.order_detail", "kafka_order_info", columns,
				"order_id",
				"'topic' = 'kaorder.kaorder.order_detail', 'properties.group.id' = 'flink-jobs_kaorder_order_detail'"));

		System.out.println();

		System.out.println(toCreateTableSQL(bidb, "order_info", columns, "order_id", null));

		System.out.println();

		System.out.println(insertSQL("order_info", "kafka_order_info", columns));

	}

}
