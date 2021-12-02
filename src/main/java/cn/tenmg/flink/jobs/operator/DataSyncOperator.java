package cn.tenmg.flink.jobs.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.exception.IllegalConfigurationException;
import cn.tenmg.flink.jobs.kit.ParamsKit;
import cn.tenmg.flink.jobs.model.DataSync;
import cn.tenmg.flink.jobs.model.data.sync.Column;
import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetter;
import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetter.TableMetaData;
import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetterFactory;
import cn.tenmg.flink.jobs.parser.FlinkSQLParamsParser;
import cn.tenmg.flink.jobs.utils.ConfigurationUtils;
import cn.tenmg.flink.jobs.utils.MapUtils;
import cn.tenmg.flink.jobs.utils.SQLUtils;

/**
 * 数据同步操作执行器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public class DataSyncOperator extends AbstractOperator<DataSync> {

	private static final String SMART_KEY = "data.sync.smart", FROM_TABLE_PREFIX_KEY = "data.sync.from_table_prefix",
			TOPIC_KEY = "topic", GROUP_ID_KEY = "properties.group.id",
			GROUP_ID_PREFIX_KEY = "data.sync.group_id_prefix",
			TIMESTAMP_COLUMN_NAME = "data.sync.timestamp.column_name",
			TIMESTAMP_FROM_TYPE_KEY = "data.sync.timestamp.from_type",
			TIMESTAMP_TO_TYPE_KEY = "data.sync.timestamp.to_type";

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
	public Object execute(StreamExecutionEnvironment env, DataSync dataSync, Map<String, Object> params)
			throws Exception {
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
		String primaryKey = collation(dataSync, fromDataSource, toDataSource, params);
		List<Column> columns = dataSync.getColumns();

		String sql = fromCreateTableSQL(fromDataSource, dataSync.getTopic(), table, fromTable, columns, primaryKey,
				fromConfig);
		System.out.println("Create source table by Flink SQL: " + sql);
		tableEnv.executeSql(sql);

		sql = toCreateTableSQL(toDataSource, table, columns, primaryKey, dataSync.getToConfig());
		System.out.println("Create sink table by Flink SQL: " + sql);
		tableEnv.executeSql(sql);

		sql = insertSQL(table, fromTable, columns);
		System.out.println("Execute Flink SQL: " + sql);
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
	 * @param params
	 *            参数查找表
	 * @return 返回主键
	 * @throws Exception
	 *             发生异常
	 */
	private static String collation(DataSync dataSync, Map<String, String> fromDataSource,
			Map<String, String> toDataSource, Map<String, Object> params) throws Exception {
		List<Column> columns = dataSync.getColumns();
		if (columns == null) {
			dataSync.setColumns(columns = new ArrayList<Column>());
		}
		Boolean smart = dataSync.getSmart();
		if (smart == null) {
			smart = Boolean.valueOf(FlinkJobsContext.getProperty(SMART_KEY));
		}
		String primaryKey = dataSync.getPrimaryKey(), timestampColumnName = dataSync.getTimestampColumnName();
		if (StringUtils.isBlank(timestampColumnName)) {// 没有指定时间戳字段名，使用配置的全局默认值
			timestampColumnName = FlinkJobsContext.getProperty(TIMESTAMP_COLUMN_NAME);
		}
		boolean needTimestampColumn = StringUtils.isNotBlank(timestampColumnName);
		if (Boolean.TRUE.equals(smart)) {// 智能模式，自动查询列名、数据类型
			MetaDataGetter metaDataGetter = MetaDataGetterFactory.getMetaDataGetter(toDataSource);
			TableMetaData tableMetaData = metaDataGetter.getTableMetaData(toDataSource, dataSync.getTable());
			Set<String> primaryKeys = tableMetaData.getPrimaryKeys();
			if (primaryKey == null && primaryKeys != null && !primaryKeys.isEmpty()) {
				primaryKey = String.join(",", primaryKeys);
			}
			Map<String, String> columnsMap = tableMetaData.getColumns();
			if (columns.isEmpty()) {// 没有用户自定义列
				needTimestampColumn = addSmartLoadColumns(columns, columnsMap, params, timestampColumnName,
						needTimestampColumn);
			} else {// 有用户自定义列
				needTimestampColumn = collationPartlyCustomColumns(columns, params, columnsMap, timestampColumnName,
						needTimestampColumn);
			}
		} else if (columns.isEmpty()) {// 没有用户自定义列
			throw new IllegalArgumentException(
					"At least one column must be configured in manual mode, or set the configuration '" + SMART_KEY
							+ "=true' at " + FlinkJobsContext.getConfigurationFile()
							+ " to enable automatic column acquisition in smart mode");
		} else {// 全部是用户自定义列
			needTimestampColumn = collationCustomColumns(columns, params, timestampColumnName, needTimestampColumn);
		}
		if (needTimestampColumn) {// 添加时间戳列
			addTimestampColumn(dataSync, timestampColumnName);
		}
		return primaryKey;
	}

	private static boolean collationPartlyCustomColumns(List<Column> columns, Map<String, Object> params,
			Map<String, String> columnsMap, String timestampColumnName, boolean needTimestampColumn) {
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

			toType = columnsMap.get(column.getToName());
			if (toType == null) {// 类型补全
				if (needTimestampColumn && column.getToName().equals(timestampColumnName)) {// 时间戳字段
					needTimestampColumn = false;
					updateTimestampColumn(column);// 更新时间戳列
				} else {
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
						column.setToType(fromType);
					}
				}
			} else {// 使用用户自定义列覆盖智能获取的列
				if (needTimestampColumn && column.getToName().equals(timestampColumnName)) {// 时间戳字段
					needTimestampColumn = false;
					updateTimestampColumn(column);// 更新时间戳列
				} else {
					if (StringUtils.isBlank(column.getToType())) {
						column.setToType(toType);
					}
					ColumnConvertArgs columnConvertArgs = columnConvertArgsMap.get(getDataType(toType).toUpperCase());

					fromType = column.getFromType();
					if (columnConvertArgs == null) {// 无类型转换配置
						if (StringUtils.isBlank(fromType)) {
							column.setFromType(toType);
						}
					} else {// 有类型转换配置
						if (StringUtils.isBlank(fromType)) {
							column.setFromType(columnConvertArgs.fromType);
							if (StringUtils.isBlank(column.getScript())) {
								column.setScript(toScript(columnConvertArgs, column.getFromName(), params));
							}
						} else {
							if (columnConvertArgs.fromType.equalsIgnoreCase(getDataType(fromType))) {
								if (StringUtils.isBlank(column.getScript())) {
									column.setScript(toScript(columnConvertArgs, column.getFromName(), params));
								}
							}
						}
					}
				}
				columnsMap.remove(column.getToName());
			}
		}
		return addSmartLoadColumns(columns, columnsMap, params, timestampColumnName, needTimestampColumn);
	}

	private static boolean collationCustomColumns(List<Column> columns, Map<String, Object> params,
			String timestampColumnName, boolean needTimestampColumn) {
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

			if (needTimestampColumn && column.getToName().equals(timestampColumnName)) {// 时间戳字段
				needTimestampColumn = false;
				updateTimestampColumn(column);// 更新时间戳列
			} else {
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
					column.setToType(fromType);
				}

				ColumnConvertArgs columnConvertArgs = columnConvertArgsMap
						.get(getDataType(column.getToType()).toUpperCase());
				if (columnConvertArgs != null
						&& columnConvertArgs.fromType.equalsIgnoreCase(getDataType(column.getFromType()))) {// 有类型转换配置
					column.setFromType(columnConvertArgs.fromType);
					if (StringUtils.isBlank(column.getScript())) {
						column.setScript(toScript(columnConvertArgs, column.getFromName(), params));
					}
				}
			}

		}
		return needTimestampColumn;
	}

	private static void updateTimestampColumn(Column timestampColumn) {
		if (StringUtils.isBlank(timestampColumn.getFromType())) {
			timestampColumn.setFromType(getDefaultTimestampColumnFromType());
		}
		if (StringUtils.isBlank(timestampColumn.getToType())) {
			timestampColumn.setToType(getDefaultTimestampColumnToType());
		}
	}

	private static void addTimestampColumn(DataSync dataSync, String timestampColumnName) {
		Column column = new Column();
		column.setFromName(timestampColumnName);
		column.setToName(timestampColumnName);// 目标字段名和来源字段名相同
		column.setFromType(getDefaultTimestampColumnFromType());
		column.setToType(getDefaultTimestampColumnToType());
		dataSync.getColumns().add(column);
	}

	private static boolean addSmartLoadColumns(List<Column> columns, Map<String, String> columnsMap,
			Map<String, Object> params, String timestampColumnName, boolean needTimestampColumn) {
		String toName, toType;
		for (Iterator<Entry<String, String>> it = columnsMap.entrySet().iterator(); it.hasNext();) {
			Entry<String, String> entry = it.next();
			toName = entry.getKey();
			toType = entry.getValue();

			Column column = new Column();
			column.setFromName(toName);// 来源字段名和目标字段名相同
			column.setToName(toName);
			column.setToType(toType);
			if (needTimestampColumn && toName.equals(timestampColumnName)) {// 时间戳字段
				needTimestampColumn = false;
				column.setFromType(getDefaultTimestampColumnFromType());
			} else {
				ColumnConvertArgs columnConvertArgs = columnConvertArgsMap.get(getDataType(toType).toUpperCase());
				if (columnConvertArgs == null) {// 无类型转换配置
					column.setFromType(toType);
				} else {// 有类型转换配置
					column.setFromType(columnConvertArgs.fromType);
					column.setScript(toScript(columnConvertArgs, column.getFromName(), params));
				}
			}
			columns.add(column);
		}
		return needTimestampColumn;
	}

	private static String getDefaultTimestampColumnFromType() {
		return FlinkJobsContext.getProperty(TIMESTAMP_FROM_TYPE_KEY);
	}

	private static String getDefaultTimestampColumnToType() {
		return FlinkJobsContext.getProperty(TIMESTAMP_TO_TYPE_KEY);
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

	/**
	 * 将同步的列转换为SELECT语句的其中一个片段
	 * 
	 * @param columnConvertArgs
	 * @param columnName
	 * @param params
	 * @return
	 */
	private static String toScript(ColumnConvertArgs columnConvertArgs, String columnName, Map<String, Object> params) {
		NamedScript namedScript = DSLUtils.parse(columnConvertArgs.script,
				ParamsKit.init(params).put("columnName", columnName).get());
		return DSLUtils.toScript(namedScript.getScript(), namedScript.getParams(), FlinkSQLParamsParser.getInstance())
				.getValue();
	}

	private static String getDataType(String type) {
		return type.split("\\s", 2)[0];
	}

	/**
	 * 列转换配置参数
	 * 
	 * @author June wjzhao@aliyun.com
	 * 
	 * @since 1.1.3
	 *
	 */
	private static class ColumnConvertArgs {

		/**
		 * 来源类型
		 */
		private String fromType;

		/**
		 * 转换的SQL脚本片段
		 */
		private String script;

		public ColumnConvertArgs(String fromType, String script) {
			super();
			this.fromType = fromType;
			this.script = script;
		}

	}

}
