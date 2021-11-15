package cn.tenmg.flink.jobs.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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
import cn.tenmg.flink.jobs.exception.IllegalConfigurationException;
import cn.tenmg.flink.jobs.model.DataSync;
import cn.tenmg.flink.jobs.model.data.sync.Column;
import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetter;
import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetter.TableMetaData;
import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetterFactory;
import cn.tenmg.flink.jobs.utils.ConfigurationUtils;
import cn.tenmg.flink.jobs.utils.MapUtils;
import cn.tenmg.flink.jobs.utils.PlaceHolderUtils;
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

	private static final Map<String, ColumnConvertArgs> columnConvertArgsMap = new HashMap<String, ColumnConvertArgs>();

	static {
		String convert = FlinkJobsContext.getProperty("data.sync.columns.convert");
		if (convert != null) {
			String argsArr[] = convert.split(";"), args[], argsStr, fromType = null, toType, script;
			StringBuilder typeBuilder = new StringBuilder();
			for (int i = 0; i < argsArr.length; i++) {
				argsStr = argsArr[i];
				int j = 0, len = argsStr.length();
				boolean sameType = false;
				while (j < len) {
					char c = argsStr.charAt(j++);
					if (c == ',') {
						fromType = typeBuilder.toString().trim();
						break;
					} else if (c == ':') {
						sameType = true;
						break;
					} else {
						typeBuilder.append(c);
					}
				}
				typeBuilder.setLength(0);

				if (sameType) {
					toType = fromType;
					script = argsStr.substring(j);
					if (StringUtils.isBlank(script)) {
						throw new IllegalConfigurationException(
								"Each item of the configuration for the key 'data.sync.columns.convert' must be in the form of '{type}:{script}' or '{fromtype},{totype}:{script}'");
					}
				} else {
					args = argsStr.substring(j).split(":", 2);
					if (args.length < 2) {
						throw new IllegalConfigurationException(
								"Each item of the configuration for the key 'data.sync.columns.convert' must be in the form of '{type}:{script}' or '{fromtype},{totype}:{script}'");
					}
					toType = args[0];
					script = args[1];
				}
				columnConvertArgsMap.put(toType.toUpperCase(), new ColumnConvertArgs(fromType, script));
			}
		}
	}

	@Override
	Object execute(StreamExecutionEnvironment env, DataSync dataSync, Map<String, Object> params) throws Exception {
		String from = dataSync.getFrom(), to = dataSync.getTo(), table = dataSync.getTable();
		if (StringUtils.isBlank(from) || StringUtils.isBlank(to) || StringUtils.isBlank(table)) {
			throw new IllegalArgumentException("The property 'from', 'to' or 'table' cannot be blank.");
		}
		StreamTableEnvironment tableEnv = FlinkJobsContext.getOrCreateStreamTableEnvironment(env);
		String currentCatalog = tableEnv.getCurrentCatalog(),
				defaultCatalog = FlinkJobsContext.getDefaultCatalog(tableEnv),
				fromTable = FlinkJobsContext.getProperty(FROM_TABLE_PREFIX_KEY) + table,
				fromConfig = dataSync.getFromConfig();
		if (!defaultCatalog.equals(currentCatalog)) {
			tableEnv.useCatalog(defaultCatalog);
		}

		Map<String, String> fromDataSource = FlinkJobsContext.getDatasource(from),
				toDataSource = FlinkJobsContext.getDatasource(to);
		String primaryKey = collation(dataSync, fromDataSource, toDataSource);
		List<Column> columns = dataSync.getColumns();
		String sql = fromCreateTableSQL(fromDataSource, dataSync.getTopic(), table, fromTable, columns, primaryKey,
				fromConfig);
		System.out.println("Execute Flink SQL:");
		System.out.println(sql);
		tableEnv.executeSql(sql);

		sql = toCreateTableSQL(toDataSource, table, columns, primaryKey, dataSync.getToConfig());
		System.out.println("Execute Flink SQL:");
		System.out.println(sql);
		tableEnv.executeSql(sql);

		sql = insertSQL(table, fromTable, columns);
		System.out.println("Execute Flink SQL:");
		System.out.println(sql);
		return tableEnv.executeSql(sql);
	}

	/**
	 * 校对和整理列配置并返回主键字段（多个字段之间使用“,”分隔）
	 * 
	 * @param dataSync
	 *            数据同步配置对象
	 * @param fromDataSource
	 *            来源数据源
	 * @param toDataSource
	 *            目标数据源
	 * @return 返回主键
	 * @throws Exception
	 *             发生异常
	 */
	private static String collation(DataSync dataSync, Map<String, String> fromDataSource,
			Map<String, String> toDataSource) throws Exception {
		List<Column> columns = dataSync.getColumns();
		Boolean smart = dataSync.getSmart();
		if (smart == null) {
			smart = Boolean.valueOf(FlinkJobsContext.getProperty(SMART_KEY));
		}
		String primaryKey = dataSync.getPrimaryKey();
		if (Boolean.TRUE.equals(smart)) {// 智能模式，自动查询列名、数据类型
			MetaDataGetter metaDataGetter = MetaDataGetterFactory.getMetaDataGetter(toDataSource);
			TableMetaData tableMetaData = metaDataGetter.getTableMetaData(toDataSource, dataSync.getTable());
			Set<String> primaryKeys = tableMetaData.getPrimaryKeys();
			if (primaryKey == null && primaryKeys != null && !primaryKeys.isEmpty()) {
				primaryKey = String.join(",", primaryKeys);
			}
			Map<String, String> columnsMap = tableMetaData.getColumns();
			if (columns == null) {// 没有用户自定义列
				columns = new ArrayList<Column>();
				dataSync.setColumns(columns);
				addColumns(columns, columnsMap);
			} else if (columns.isEmpty()) {// 没有用户自定义列
				addColumns(columns, columnsMap);
			} else {// 有用户自定义列
				String toName, fromName, fromType, toType;
				for (int i = 0, size = columns.size(); i < size; i++) {
					Column column = columns.get(i);
					fromName = column.getFromName();
					toName = column.getToName();
					if (StringUtils.isBlank(fromName)) {
						if (StringUtils.isBlank(toName)) {
							throw new IllegalArgumentException(
									"One of the properties 'fromName' or 'toName' cannot be blank, column index: " + i);
						} else {
							column.setFromName(toName);
						}
					} else if (StringUtils.isBlank(toName)) {
						column.setToName(fromName);
					}

					toType = columnsMap.get(toName);
					if (toType == null) {// 类型补全
						fromType = column.getFromType();
						toType = column.getToType();
						if (StringUtils.isBlank(fromType)) {
							if (StringUtils.isBlank(toType)) {
								throw new IllegalArgumentException(
										"One of the properties 'fromType' or 'toType' cannot be blank, column index: "
												+ i);
							} else {
								column.setFromType(toType);
							}
						} else if (StringUtils.isBlank(toType)) {
							column.setToType(toType);
						}
					} else {// 使用用户自定义列覆盖智能获取的列
						if (StringUtils.isBlank(column.getToType())) {
							column.setToType(toType);
						}
						ColumnConvertArgs columnConvertArgs = columnConvertArgsMap.get(toType.toUpperCase());
						if (columnConvertArgs == null) {// 无类型转换配置
							column.setFromType(toType);
						} else {// 有类型转换配置
							fromType = column.getFromType();
							if (StringUtils.isBlank(fromType)) {
								column.setFromType(columnConvertArgs.fromType);
								column.setScript(PlaceHolderUtils.replace(columnConvertArgs.script, "${columnName}",
										column.getFromName()));
							} else if (columnConvertArgs.fromType.equalsIgnoreCase(fromType)) {
								column.setScript(PlaceHolderUtils.replace(columnConvertArgs.script, "${columnName}",
										column.getFromName()));
							}
						}
						columnsMap.remove(toName);
					}
				}
				addColumns(columns, columnsMap);
			}
		} else if (columns == null || columns.isEmpty()) {// 没有用户自定义列
			throw new IllegalArgumentException(
					"At least one column must be configured in manual mode, or set the configuration '" + SMART_KEY
							+ "=true' at " + FlinkJobsContext.getConfigurationFile()
							+ " to enable automatic column acquisition in smart mode");
		} else {
			String fromName, toName, fromType, toType;
			for (int i = 0, size = columns.size(); i < size; i++) {
				Column column = columns.get(i);
				fromName = column.getFromName();
				toName = column.getToName();
				if (StringUtils.isBlank(fromName)) {
					if (StringUtils.isBlank(toName)) {
						throw new IllegalArgumentException(
								"One of the properties 'fromName' or 'toName' cannot be blank, column index: " + i);
					} else {
						column.setFromName(toName);
					}
				} else if (StringUtils.isBlank(toName)) {
					column.setToName(fromName);
				}

				fromType = column.getFromType();
				toType = column.getToType();
				if (StringUtils.isBlank(fromType)) {
					if (StringUtils.isBlank(toType)) {
						throw new IllegalArgumentException(
								"One of the properties 'fromType' or 'toType' cannot be blank, column index: " + i);
					} else {
						column.setFromType(toType);
					}
				} else if (StringUtils.isBlank(toType)) {
					column.setToType(toType);
				}
				ColumnConvertArgs columnConvertArgs = columnConvertArgsMap.get(column.getToType().toUpperCase());
				if (columnConvertArgs == null) {// 无类型转换配置
					column.setFromType(toType);
				} else if (columnConvertArgs.fromType.equalsIgnoreCase(column.getFromType())) {// 有类型转换配置
					column.setFromType(columnConvertArgs.fromType);
					column.setScript(
							PlaceHolderUtils.replace(columnConvertArgs.script, "${columnName}", column.getFromName()));
				}
			}
		}
		return primaryKey;
	}

	private static void addColumns(List<Column> columns, Map<String, String> columnsMap) {
		for (Iterator<Entry<String, String>> it = columnsMap.entrySet().iterator(); it.hasNext();) {
			Entry<String, String> column = it.next();
			addColumn(columns, column.getKey(), column.getValue());
		}
	}

	private static void addColumn(List<Column> columns, String toName, String toType) {
		Column column = new Column();
		column.setFromName(toName);// 原来字段名和目标字段名相同
		column.setToName(toName);
		column.setToType(toType);
		ColumnConvertArgs columnConvertArgs = columnConvertArgsMap.get(toType.toUpperCase());
		if (columnConvertArgs == null) {// 无类型转换配置
			column.setFromType(toType);
		} else {// 有类型转换配置
			column.setFromType(columnConvertArgs.fromType);
			column.setScript(PlaceHolderUtils.replace(columnConvertArgs.script, "${columnName}", toName));
		}
		columns.add(column);
	}

	private static String fromCreateTableSQL(Map<String, String> dataSource, String topic, String table,
			String fromTable, List<Column> columns, String primaryKey, String fromConfig) throws IOException {
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
		Map<String, String> actualDataSource = MapUtils.newHashMap(dataSource);
		if (StringUtils.isBlank(fromConfig)) {
			actualDataSource.put(GROUP_ID_KEY, FlinkJobsContext.getProperty(GROUP_ID_PREFIX_KEY) + table);// 设置properties.group.id
			if (topic != null) {
				actualDataSource.put(TOPIC_KEY, topic);
			}
			SQLUtils.appendDataSource(sqlBuffer, actualDataSource);
		} else {
			Map<String, String> config = ConfigurationUtils.load(fromConfig);
			MapUtils.removeAll(actualDataSource, config.keySet());
			if (!config.containsKey(GROUP_ID_KEY)) {
				actualDataSource.put(GROUP_ID_KEY, FlinkJobsContext.getProperty(GROUP_ID_PREFIX_KEY) + table);// 设置properties.group.id
			}
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
		String toName = column.getToName();
		sqlBuffer.append(toName == null ? column.getFromName() : toName).append(DSLUtils.BLANK_SPACE)
				.append(column.getToType());
		for (int i = 1, size = columns.size(); i < size; i++) {
			column = columns.get(i);
			toName = column.getToName();
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE)
					.append(toName == null ? column.getFromName() : toName).append(DSLUtils.BLANK_SPACE)
					.append(column.getToType());
		}
		if (StringUtils.isNotBlank(primaryKey)) {
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append("PRIMARY KEY (").append(primaryKey)
					.append(") NOT ENFORCED");
		}

		sqlBuffer.append(") ").append("WITH (");
		Map<String, String> actualDataSource = MapUtils.newHashMap(dataSource);
		actualDataSource.put("table-name", table);
		if (StringUtils.isBlank(toConfig)) {
			SQLUtils.appendDataSource(sqlBuffer, actualDataSource);
		} else {
			Map<String, String> config = ConfigurationUtils.load(toConfig);
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
		sqlBuffer.append(StringUtils.isBlank(script) ? column.getFromName() : script);
		for (int i = 1, size = columns.size(); i < size; i++) {
			column = columns.get(i);
			script = column.getScript();
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE)
					.append(StringUtils.isBlank(script) ? column.getFromName() : script);
		}

		sqlBuffer.append(" FROM ").append(fromTable);
		return sqlBuffer.toString();

	}

	private static class ColumnConvertArgs {

		private String fromType;

		private String script;

		public ColumnConvertArgs(String fromType, String script) {
			super();
			this.fromType = fromType;
			this.script = script;
		}

	}

}
