package cn.tenmg.flink.jobs.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.model.DataSync;
import cn.tenmg.flink.jobs.model.data.sync.Column;
import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetter;
import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetter.TableMetaData;
import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetterFactory;
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

	private static final String SMART_KEY = "data.sync.smart", FROM_TABLE_PREFIX_KEY = "data.sync.from_table_prefix",
			TOPIC_KEY = "topic", GROUP_ID_KEY = "properties.group.id",
			GROUP_ID_PREFIX_KEY = "data.sync.group_id_prefix";

	@Override
	Object execute(StreamExecutionEnvironment env, DataSync dataSync, Map<String, Object> params) throws Exception {
		String from = dataSync.getFrom(), to = dataSync.getTo(), table = dataSync.getTable();
		if (StringUtils.isBlank(from) || StringUtils.isBlank(to) || StringUtils.isBlank(table)) {
			throw new IllegalArgumentException("The property 'from', 'to' or 'table' cannot be blank.");
		}
		StreamTableEnvironment tableEnv = FlinkJobsContext.getOrCreateStreamTableEnvironment(env);
		String primaryKey = dataSync.getPrimaryKey(), topic = dataSync.getTopic(),
				currentCatalog = tableEnv.getCurrentCatalog(),
				defaultCatalog = FlinkJobsContext.getDefaultCatalog(tableEnv),
				fromTable = FlinkJobsContext.get(FROM_TABLE_PREFIX_KEY) + table,

				fromConfig = dataSync.getFromConfig();
		if (!defaultCatalog.equals(currentCatalog)) {
			tableEnv.useCatalog(defaultCatalog);
		}
		Map<String, String> fromDataSource = MapUtils.newHashMap(FlinkJobsContext.getDatasource(from)),
				toDataSource = FlinkJobsContext.getDatasource(to);
		if (StringUtils.isBlank(fromConfig)) {// 设置properties.group.id
			fromDataSource.put(GROUP_ID_KEY, FlinkJobsContext.getProperty(GROUP_ID_PREFIX_KEY) + table);
		}

		List<Column> columns = dataSync.getColumns();
		Boolean smart = dataSync.getSmart();
		if (smart == null) {
			smart = Boolean.valueOf(FlinkJobsContext.getProperty(SMART_KEY));
		}
		if (Boolean.TRUE.equals(smart)) {// 智能模式，自动查询列名、数据类型
			MetaDataGetter metaDataGetter = MetaDataGetterFactory.getMetaDataGetter(toDataSource);
			TableMetaData tableMetaData = metaDataGetter.getTableMetaData(toDataSource, table);
			Set<String> primaryKeys = tableMetaData.getPrimaryKeys();
			if (primaryKey == null && primaryKeys != null && !primaryKeys.isEmpty()) {
				primaryKey = String.join(",", primaryKeys);
			}
			Map<String, String> columnsMap = tableMetaData.getColumns();
			if (columns == null) {
				columns = new ArrayList<Column>();
				addColumns(columns, columnsMap);
			} else if (columns.isEmpty()) {
				addColumns(columns, columnsMap);
			} else {
				String toName;
				for (int i = 0, size = columns.size(); i < size; i++) {
					Column column = columns.get(i);
					toName = column.getToName();
					if (StringUtils.isBlank(toName)) {
						toName = column.getFromName();
					}
					String toType = columnsMap.get(toName);
					if (toType != null) {
						if (StringUtils.isBlank(column.getFromType())) {
							column.setFromType(toType);
						}
						if (StringUtils.isBlank(column.getToType())) {
							column.setToType(toType);
						}
						columnsMap.remove(toName);
					}
				}
				addColumns(columns, columnsMap);
			}
		}
		tableEnv.executeSql(fromCreateTableSQL(fromDataSource, topic, fromTable, columns, primaryKey, fromConfig));
		tableEnv.executeSql(toCreateTableSQL(toDataSource, table, columns, primaryKey, dataSync.getToConfig()));
		return tableEnv.executeSql(insertSQL(table, fromTable, columns));
	}

	private static void addColumns(List<Column> columns, Map<String, String> columnsMap) {
		for (Iterator<Entry<String, String>> it = columnsMap.entrySet().iterator(); it.hasNext();) {
			Entry<String, String> column = it.next();
			addColumn(columns, column.getKey(), column.getValue());
		}
	}

	private static void addColumn(List<Column> columns, String fromName, String fromType) {
		Column column = new Column();
		column.setFromName(fromName);
		column.setFromType(fromType);
		columns.add(column);
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
			if (topic != null) {
				dataSource.put(TOPIC_KEY, topic);
			}
			SQLUtils.appendDataSource(sqlBuffer, dataSource);
		} else {
			Map<String, String> config = ConfigurationUtils.load(fromConfig);
			MapUtils.removeAll(dataSource, config.keySet());
			if (topic != null && !config.containsKey(TOPIC_KEY)) {
				dataSource.put(TOPIC_KEY, topic);
			}
			SQLUtils.appendDataSource(sqlBuffer, dataSource);
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

}
