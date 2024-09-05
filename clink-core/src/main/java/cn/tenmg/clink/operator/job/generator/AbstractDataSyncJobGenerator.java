package cn.tenmg.clink.operator.job.generator;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.clink.exception.IllegalConfigurationException;
import cn.tenmg.clink.exception.IllegalJobConfigException;
import cn.tenmg.clink.metadata.MetaDataGetter.TableMetaData.ColumnType;
import cn.tenmg.clink.model.DataSync;
import cn.tenmg.clink.model.data.sync.Column;
import cn.tenmg.clink.operator.job.DataSyncJobGenerator;
import cn.tenmg.clink.parser.FlinkSQLParamsParser;
import cn.tenmg.clink.utils.SQLUtils;
import cn.tenmg.clink.utils.StreamTableEnvironmentUtils;
import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 抽象数据同步作业生成器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public abstract class AbstractDataSyncJobGenerator implements DataSyncJobGenerator {

	protected static final String TOPIC_KEY = "topic", GROUP_ID_KEY = "properties.group.id",
			AUTO_COLUMNS = "data.sync.auto-columns", AUTO_COLUMNS_SPLITER = ",",
			TYPE_KEY_PREFIX = "data.sync" + ClinkContext.CONFIG_SPLITER,
			SCRIPT_KEY_SUFFIX = ClinkContext.CONFIG_SPLITER + "script",
			STRATEGY_KEY_SUFFIX = ClinkContext.CONFIG_SPLITER + "strategy", COLUMN_NAME = "columnName";

	protected static final boolean TO_LOWERCASE = !Boolean
			.valueOf(ClinkContext.getProperty("data.sync.case-sensitive", "true"));// 不区分大小写，统一转为小写

	protected static final Map<String, ColumnConvert> columnConverts = MapUtils.newHashMap();

	protected static final Pattern METADATA_PATTERN = Pattern.compile("METADATA[\\s]+FROM[\\s]+'[\\S]+'[\\s]+VIRTUAL");

	static {
		String convert = ClinkContext.getProperty("data.sync.columns.convert");
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
				columnConverts.put(toType.toUpperCase(), new ColumnConvert(fromType, script));
			}
		}
	}

	protected Logger log = LoggerFactory.getLogger(getClass());

	/**
	 * 生成数据同步任务
	 * 
	 * @param env              流执行环境
	 * @param tableEnv         流表执行环境
	 * @param dataSync         同步配置对象
	 * @param sourceDataSource 源数据源
	 * @param sinkDataSource   汇数据源
	 * @param params           参数查找表
	 * @return 生成数据同步任务的结果
	 * @throws Exception 发生异常
	 */
	abstract Object generate(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, DataSync dataSync,
			Map<String, String> sourceDataSource, Map<String, String> sinkDataSource, Map<String, Object> params)
			throws Exception;

	@Override
	public Object generate(StreamExecutionEnvironment env, DataSync dataSync, Map<String, String> sourceDataSource,
			Map<String, String> sinkDataSource, Map<String, Object> params) throws Exception {
		StreamTableEnvironment tableEnv = ClinkContext.getOrCreateStreamTableEnvironment(env);
		StreamTableEnvironmentUtils.useDefaultCatalog(tableEnv);
		return generate(env, tableEnv, dataSync, sourceDataSource, sinkDataSource, params);
	}

	/**
	 * 生成创建数据汇表的 Flink SQL
	 * 
	 * @param dataSource  数据源
	 * @param table       表名
	 * @param columns     列
	 * @param primaryKeys 主键
	 * @param params      运行参数
	 * @return 创建数据汇表的 Flink SQL
	 * @throws IOException I/O 异常
	 */
	protected static String sinkTableSQL(Map<String, String> dataSource, String table, List<Column> columns,
			Set<String> primaryKeys, Map<String, Object> params) throws IOException {
		return sinkTableSQL(dataSource, table, columns, primaryKeys, params, DefaultPrimaryKeysCollector.getInstance());
	}

	/**
	 * 生成创建数据汇表的 Flink SQL
	 * 
	 * @param sinkDataSource 汇数据源
	 * @param table          表名
	 * @param columns        列
	 * @param primaryKeys    主键
	 * @param params         运行参数
	 * @param collector      主键收集器
	 * @return 创建数据汇表的 Flink SQL
	 * @throws IOException I/O 异常
	 */
	protected static String sinkTableSQL(Map<String, String> sinkDataSource, String table, List<Column> columns,
			Set<String> primaryKeys, Map<String, Object> params, PrimaryKeysCollector collector) throws IOException {
		Set<String> actualPrimaryKeys = newSet(primaryKeys);
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("CREATE TABLE ").append(SQLUtils.wrapIfReservedKeywords(table)).append("(");
		Column column;
		String toName, columnName;
		int i = 0, size = columns.size();
		while (i < size) {
			column = columns.get(i++);
			toName = column.getToName();
			columnName = toName == null ? column.getFromName() : toName;
			if (Strategy.FROM.equals(column.getStrategy())) {
				actualPrimaryKeys.remove(columnName);
			} else {
				sqlBuffer.append(columnName).append(DSLUtils.BLANK_SPACE).append(column.getToType());
				break;
			}
		}
		while (i < size) {
			column = columns.get(i++);
			toName = column.getToName();
			columnName = toName == null ? column.getFromName() : toName;
			if (Strategy.FROM.equals(column.getStrategy())) {
				actualPrimaryKeys.remove(columnName);
			} else {
				sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(columnName)
						.append(DSLUtils.BLANK_SPACE).append(column.getToType());
			}
		}
		if (!actualPrimaryKeys.isEmpty()) {
			collector.collect(actualPrimaryKeys);
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append("PRIMARY KEY (")
					.append(String.join(", ", actualPrimaryKeys)).append(") NOT ENFORCED");
		}
		sqlBuffer.append(") ").append("WITH (");
		SQLUtils.appendDataSource(sqlBuffer, sinkDataSource, table);
		sqlBuffer.append(")");
		return sqlBuffer.toString();
	}

	protected static void addSmartLoadColumns(String connector, List<Column> columns,
			Map<String, ColumnType> columnTypes, Map<String, Object> params, Map<String, String> autoColumnses) {
		String toName, toType, columnName, strategy;
		Entry<String, ColumnType> entry;
		for (Iterator<Entry<String, ColumnType>> it = columnTypes.entrySet().iterator(); it.hasNext();) {
			entry = it.next();
			toName = entry.getKey();
			toType = SQLUtils.toSQLType(connector, entry.getValue());

			Column column = new Column();
			column.setToName(toName);
			column.setToType(toType);
			columnName = TO_LOWERCASE ? toName.toLowerCase() : toName;// 不区分大小写，统一转为小写
			if (autoColumnses.containsKey(columnName)) {// 自动添加列
				strategy = getDefaultStrategy(columnName);
				column.setStrategy(strategy);// 设置自动添加列的同步策略
				if (!Strategy.TO.equals(strategy)) {// 非仅创建目标列
					column.setFromName(toName);// 来源列名和目标列名相同
					column.setFromType(getDefaultFromType(columnName));
				}
				if (!Strategy.FROM.equals(strategy) && StringUtils.isBlank(column.getScript())) {
					column.setScript(getDefaultScript(columnName));
				}
				autoColumnses.remove(columnName);
			} else {
				column.setFromName(toName);// 来源列名和目标列名相同
				ColumnConvert columnConvert = columnConverts.get(getDataType(toType).toUpperCase());
				if (columnConvert == null) {// 无类型转换配置
					column.setFromType(toType);
				} else {// 有类型转换配置
					column.setFromType(columnConvert.getFromType());
					column.setScript(columnConvert.getScript());
				}
			}
			wrapColumnName(column);// SQL保留关键字包装
			columns.add(column);
		}
	}

	protected static void collationPartlyCustom(String connector, List<Column> columns, Map<String, Object> params,
			Map<String, ColumnType> columnsTypes, Map<String, String> autoColumnses) {
		String strategy;
		for (int i = 0, size = columns.size(); i < size; i++) {
			Column column = columns.get(i);
			strategy = column.getStrategy();
			if (Strategy.FROM.equals(strategy)) {// 仅创建来源列
				collationPartlyCustomFromStrategy(column, params, columnsTypes, autoColumnses);
			} else if (Strategy.TO.equals(strategy)) {// 仅创建目标列
				collationPartlyCustomToStratagy(connector, column, params, columnsTypes, autoColumnses);
			} else {
				collationPartlyCustomBothStratagy(connector, column, params, columnsTypes, autoColumnses);
			}
			wrapColumnName(column);// SQL保留关键字包装
		}
		addSmartLoadColumns(connector, columns, columnsTypes, params, autoColumnses);
	}

	protected static void collationCustom(List<Column> columns, Map<String, Object> params,
			Map<String, String> autoColumnses) {
		String strategy;
		for (int i = 0, size = columns.size(); i < size; i++) {
			Column column = columns.get(i);
			strategy = column.getStrategy();
			if (Strategy.FROM.equals(strategy)) {// 仅创建来源列
				collationCustomFromStrategy(column, params, autoColumnses);
			} else if (Strategy.TO.equals(strategy)) {// 仅创建目标列
				collationCustomToStrategy(column, params, autoColumnses);
			} else {
				collationCustomBothStrategy(column, params, autoColumnses);
			}
			wrapColumnName(column);// SQL保留关键字包装
		}
	}

	protected static final Map<String, String> toMap(boolean toLowercase, String... strings) {
		Map<String, String> map = MapUtils.newHashMap();
		String string;
		if (toLowercase) {
			for (int i = 0; i < strings.length; i++) {
				string = strings[i].trim();
				map.put(string.toLowerCase(), string);
			}
		} else {
			for (int i = 0; i < strings.length; i++) {
				string = strings[i].trim();
				map.put(string, string);
			}
		}
		return map;
	}

	protected static String insertSQL(String toTable, String fromTable, List<Column> columns,
			Map<String, Object> params) {
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("INSERT INTO ").append(SQLUtils.wrapIfReservedKeywords(toTable)).append(DSLUtils.BLANK_SPACE)
				.append("(");

		boolean needComma = false;
		Column column;
		String toName;
		for (int i = 0, size = columns.size(); i < size; i++) {
			column = columns.get(i);
			toName = column.getToName();
			if (!Strategy.FROM.equals(column.getStrategy())) {
				if (needComma) {
					sqlBuffer.append(DSLUtils.COMMA);
				} else {
					needComma = true;
				}
				sqlBuffer.append(DSLUtils.BLANK_SPACE).append(toName == null ? column.getFromName() : toName);
			}
		}

		sqlBuffer.append(") SELECT ");
		needComma = false;
		String script;
		for (int i = 0, size = columns.size(); i < size; i++) {
			column = columns.get(i);
			script = column.getScript();
			if (!Strategy.FROM.equals(column.getStrategy())) {
				if (needComma) {
					sqlBuffer.append(DSLUtils.COMMA);
				} else {
					needComma = true;
				}
				sqlBuffer.append(DSLUtils.BLANK_SPACE).append(StringUtils.isBlank(script) ? column.getFromName()
						: toScript(script, column.getFromName(), params));
			}
		}

		sqlBuffer.append(" FROM ").append(SQLUtils.wrapIfReservedKeywords(fromTable));
		return sqlBuffer.toString();

	}

	protected static String getDefaultAutoColumns() {
		return ClinkContext.getProperty(AUTO_COLUMNS);
	}

	protected static String getDefaultStrategy(String columnName) {
		return ClinkContext.getProperty(StringUtils.concat(TYPE_KEY_PREFIX, columnName, STRATEGY_KEY_SUFFIX));
	}

	protected static String getDefaultFromType(String columnName) {
		String prefix = StringUtils.concat(TYPE_KEY_PREFIX, columnName, ClinkContext.CONFIG_SPLITER),
				fromType = ClinkContext.getProperty(prefix.concat("from-type"));
		if (fromType == null) {
			return ClinkContext.getProperty("data.sync.from-type");
		}
		return fromType;
	}

	protected static String getDefaultScript(String columnName) {
		return ClinkContext.getProperty(StringUtils.concat(TYPE_KEY_PREFIX, columnName, SCRIPT_KEY_SUFFIX));
	}

	protected static String getDefaultToType(String columnName) {
		String prefix = StringUtils.concat(TYPE_KEY_PREFIX, columnName, ClinkContext.CONFIG_SPLITER),
				toType = ClinkContext.getProperty(prefix.concat("to-type"));
		if (toType == null) {
			return ClinkContext.getProperty("data.sync.to-type");
		}
		return toType;
	}

	private static void collationPartlyCustomFromStrategy(Column column, Map<String, Object> params,
			Map<String, ColumnType> columnsTypes, Map<String, String> autoColumnses) {
		String fromName = column.getFromName(), fromType = column.getFromType(),
				columnName = TO_LOWERCASE ? fromName.toLowerCase() : fromName;// 不区分大小写，统一转为小写
		if (autoColumnses.containsKey(columnName)) {// 自动添加列
			if (StringUtils.isBlank(fromType)) {
				column.setFromType(getDefaultFromType(columnName));// 更新自动添加列来源类型
			}
			autoColumnses.remove(columnName);
		} else if (StringUtils.isBlank(fromType)) {
			throw new IllegalJobConfigException(StringUtils.concat(
					"Due to the 'strategy' is 'from', the property 'fromType' of the column wich 'fromName' is '",
					fromName, "' cannot be blank"));
		}
		columnsTypes.remove(fromName);
	}

	private static void collationPartlyCustomToStratagy(String connector, Column column, Map<String, Object> params,
			Map<String, ColumnType> columnsTypes, Map<String, String> autoColumnses) {
		String toName = column.getToName();
		if (StringUtils.isBlank(toName)) {
			throw new IllegalJobConfigException(StringUtils.concat(
					"Due to the 'strategy' is 'to', the property 'toName' of the column wich 'fromName' is '",
					column.getFromName(), "' cannot be blank"));
		}
		String columnName = TO_LOWERCASE ? toName.toLowerCase() : toName;// 不区分大小写，统一转为小写
		ColumnType columnType = columnsTypes.get(toName);
		if (autoColumnses.containsKey(columnName)) {// 自动添加列
			if (StringUtils.isBlank(column.getToType())) {
				column.setToType(
						columnType == null ? getDefaultToType(columnName) : SQLUtils.toSQLType(connector, columnType));
			}
			if (StringUtils.isBlank(column.getScript())) {
				column.setScript(getDefaultScript(columnName));
			}
			autoColumnses.remove(columnName);
		} else {
			if (columnType == null && StringUtils.isBlank(column.getToType())) {
				throw new IllegalJobConfigException(StringUtils.concat(
						"Due to the 'strategy' is 'to', the property 'toType' of the column wich 'fromName' is '",
						column.getFromName(), "' cannot be blank"));
			} else {// 使用用户自定义列覆盖智能获取的列
				if (StringUtils.isBlank(column.getToType())) {
					column.setToType(SQLUtils.toSQLType(connector, columnType));
				}
			}
		}
		columnsTypes.remove(toName);
	}

	private static void collationPartlyCustomBothStratagy(String connector, Column column, Map<String, Object> params,
			Map<String, ColumnType> columnTypes, Map<String, String> autoColumnses) {
		String fromName = column.getFromName(), toName = column.getToName();
		if (StringUtils.isBlank(fromName)) {
			column.setFromName(toName);
		} else if (StringUtils.isBlank(toName)) {
			column.setToName(fromName);
		}
		String columnName = TO_LOWERCASE ? column.getToName().toLowerCase() : column.getToName();// 不区分大小写，统一转为小写
		String fromType, toType = SQLUtils.toSQLType(connector, columnTypes.get(column.getToName()));
		if (autoColumnses.containsKey(columnName)) {// 自动添加列
			if (StringUtils.isBlank(column.getFromType())) {
				column.setFromType(getDefaultFromType(columnName));
			}
			if (StringUtils.isBlank(column.getToType())) {
				column.setToType(toType == null ? getDefaultToType(columnName) : toType);
			}
			if (StringUtils.isBlank(column.getScript())) {
				column.setScript(getDefaultScript(columnName));
			}
			autoColumnses.remove(columnName);
		} else {
			if (toType == null) {// 类型补全
				fromType = column.getFromType();
				toType = column.getToType();
				if (StringUtils.isBlank(fromType)) {
					if (StringUtils.isBlank(toType)) {
						throw new IllegalJobConfigException(StringUtils.concat(
								"One of the properties 'fromType' or 'toType' of the column wich 'fromName' is '",
								column.getFromName(), "' cannot be blank"));
					} else {
						column.setFromType(toType);
					}
				} else if (StringUtils.isBlank(toType)) {
					column.setToType(fromType);
				}
			} else {// 使用用户自定义列覆盖智能获取的列
				if (StringUtils.isBlank(column.getToType())) {
					column.setToType(toType);
				}
				ColumnConvert columnConvert = columnConverts.get(getDataType(toType).toUpperCase());
				fromType = column.getFromType();
				if (columnConvert == null) {// 无类型转换配置
					if (StringUtils.isBlank(fromType)) {
						column.setFromType(toType);
					}
				} else {// 有类型转换配置
					if (StringUtils.isBlank(fromType)) {
						column.setFromType(columnConvert.getFromType());
						if (StringUtils.isBlank(column.getScript())) {
							column.setScript(columnConvert.getScript());
						}
					} else {
						if (columnConvert.getFromType().equalsIgnoreCase(getDataType(fromType))) {
							if (StringUtils.isBlank(column.getScript())) {
								column.setScript(columnConvert.getScript());
							}
						}
					}
				}
				columnTypes.remove(column.getToName());
			}
		}
	}

	private static void collationCustomFromStrategy(Column column, Map<String, Object> params,
			Map<String, String> autoColumnses) {
		String fromName = column.getFromName(), fromType = column.getFromType(),
				columnName = TO_LOWERCASE ? fromName.toLowerCase() : fromName;// 不区分大小写，统一转为小写
		if (autoColumnses.containsKey(columnName)) {// 自动添加列
			if (StringUtils.isBlank(fromType)) {
				column.setFromType(getDefaultFromType(columnName));// 更新自动添加列来源类型
			}
			autoColumnses.remove(columnName);
		} else if (StringUtils.isBlank(fromType)) {
			throw new IllegalJobConfigException(StringUtils.concat(
					"Due to the 'strategy' is 'from', the property 'fromType' of the column wich 'fromName' is '",
					fromName, "' cannot be blank"));
		}
	}

	private static void collationCustomToStrategy(Column column, Map<String, Object> params,
			Map<String, String> autoColumnses) {
		String toName = column.getToName();
		if (StringUtils.isBlank(toName)) {
			throw new IllegalJobConfigException(StringUtils.concat(
					"Due to the 'strategy' is 'to', the property 'toName' of the column wich 'fromName' is '",
					column.getFromName(), "' cannot be blank"));
		}
		String columnName = TO_LOWERCASE ? toName.toLowerCase() : toName;// 不区分大小写，统一转为小写
		if (autoColumnses.containsKey(columnName)) {// 自动添加列
			if (StringUtils.isBlank(column.getToType())) {
				column.setToType(getDefaultToType(columnName));
			}
			if (StringUtils.isBlank(column.getScript())) {
				column.setScript(getDefaultScript(columnName));
			}
			autoColumnses.remove(columnName);
		} else if (StringUtils.isBlank(column.getToType())) {
			throw new IllegalJobConfigException(StringUtils.concat(
					"Due to the 'strategy' is 'to', the property 'toType' of the column wich 'fromName' is '",
					column.getFromName(), "' cannot be blank"));
		}
	}

	private static void collationCustomBothStrategy(Column column, Map<String, Object> params,
			Map<String, String> autoColumnses) {
		String fromName = column.getFromName(), toName = column.getToName();
		if (StringUtils.isBlank(fromName)) {
			column.setFromName(toName);
		} else if (StringUtils.isBlank(toName)) {
			column.setToName(fromName);
		}
		String columnName = TO_LOWERCASE ? column.getToName().toLowerCase() : column.getToName();// 不区分大小写，统一转为小写
		if (autoColumnses.containsKey(columnName)) {// 自动添加列
			if (StringUtils.isBlank(column.getFromType())) {
				column.setFromType(getDefaultFromType(columnName));
			}
			if (StringUtils.isBlank(column.getToType())) {
				column.setToType(getDefaultToType(columnName));
			}
			if (StringUtils.isBlank(column.getScript())) {
				column.setScript(getDefaultScript(columnName));
			}
			autoColumnses.remove(columnName);
		} else {
			String fromType = column.getFromType(), toType = column.getToType();
			if (StringUtils.isBlank(fromType)) {
				if (StringUtils.isBlank(toType)) {
					throw new IllegalJobConfigException(StringUtils.concat(
							"One of the properties 'fromType' or 'toType' of the column wich 'fromName' is '",
							column.getFromName(), "' cannot be blank"));
				} else {
					column.setFromType(toType);
				}
			} else if (StringUtils.isBlank(toType)) {
				column.setToType(fromType);
			}

			ColumnConvert columnConvert = columnConverts.get(getDataType(column.getToType()).toUpperCase());
			if (columnConvert != null
					&& columnConvert.getFromType().equalsIgnoreCase(getDataType(column.getFromType()))) {// 有类型转换配置
				column.setFromType(columnConvert.getFromType());
				if (StringUtils.isBlank(column.getScript())) {
					column.setScript(columnConvert.getScript());
				}
			}
		}
	}

	private static String getDataType(String type) {
		return type.split("\\s", 2)[0];
	}

	// 包装SQL保留关键字
	private static void wrapColumnName(Column column) {
		column.setFromName(SQLUtils.wrapIfReservedKeywords(column.getFromName()));
		column.setToName(SQLUtils.wrapIfReservedKeywords(column.getToName()));
	}

	private static Set<String> newSet(Set<String> set) {
		Set<String> newSet = new HashSet<String>();
		if (set != null) {
			newSet.addAll(set);
		}
		return newSet;
	}

	// 将同步的列转换为SELECT语句的其中一个片段
	private static String toScript(String dsl, String columnName, Map<String, Object> params) {
		NamedScript namedScript = DSLUtils.parse(dsl, MapUtils.toHashMapBuilder(params).build(COLUMN_NAME, columnName));
		return DSLUtils.toScript(namedScript.getScript(), namedScript.getParams(), FlinkSQLParamsParser.getInstance())
				.getValue();
	}

	/**
	 * 同步策略
	 * 
	 * @author June wjzhao@aliyun.com
	 * 
	 * @since 1.6.0
	 */
	protected static class Strategy {
		public static final String TO = "to", FROM = "from", BOTH = "both";
	}

	/**
	 * 列转换配置参数
	 * 
	 * @author June wjzhao@aliyun.com
	 * 
	 * @since 1.6.0
	 *
	 */
	protected static class ColumnConvert {

		/**
		 * 来源类型
		 */
		private String fromType;

		/**
		 * 转换的SQL脚本片段
		 */
		private String script;

		public String getFromType() {
			return fromType;
		}

		public String getScript() {
			return script;
		}

		public ColumnConvert(String fromType, String script) {
			super();
			this.fromType = fromType;
			this.script = script;
		}

	}

	/**
	 * 主键收集器
	 * 
	 * @author June wjzhao@aliyun.com
	 * 
	 * @since 1.6.0
	 */
	protected interface PrimaryKeysCollector {
		/**
		 * 收集主键列名
		 * 
		 * @param primaryKeys 主键列名
		 */
		void collect(Set<String> primaryKeys);
	}

	/**
	 * 默认主键收集器
	 * 
	 * @author June wjzhao@aliyun.com
	 * 
	 * @since 1.6.0
	 */
	protected static class DefaultPrimaryKeysCollector implements PrimaryKeysCollector {

		private static final DefaultPrimaryKeysCollector INSTANCE = new DefaultPrimaryKeysCollector();

		public static DefaultPrimaryKeysCollector getInstance() {
			return INSTANCE;
		}

		private DefaultPrimaryKeysCollector() {
		}

		@Override
		public void collect(Set<String> primaryKeys) {
		}

	}

}
