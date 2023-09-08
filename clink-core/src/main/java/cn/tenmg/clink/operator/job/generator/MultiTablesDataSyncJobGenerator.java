package cn.tenmg.clink.operator.job.generator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.clink.datasource.DataSourceConverter;
import cn.tenmg.clink.exception.IllegalConfigurationException;
import cn.tenmg.clink.exception.IllegalJobConfigException;
import cn.tenmg.clink.metadata.MetaDataGetter;
import cn.tenmg.clink.metadata.MetaDataGetter.TableMetaData;
import cn.tenmg.clink.metadata.MetaDataGetter.TableMetaData.ColumnType;
import cn.tenmg.clink.metadata.MetaDataGetterFactory;
import cn.tenmg.clink.model.DataSync;
import cn.tenmg.clink.model.data.sync.Column;
import cn.tenmg.clink.source.SourceFactory;
import cn.tenmg.clink.table.functions.MultiTablesRowsFilterFunction;
import cn.tenmg.clink.utils.ConfigurationUtils;
import cn.tenmg.clink.utils.DataTypeUtils;
import cn.tenmg.clink.utils.SQLUtils;
import cn.tenmg.dsl.utils.CollectionUtils;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.ObjectUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 多表数据同步作业生成器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class MultiTablesDataSyncJobGenerator extends AbstractDataSyncJobGenerator {

	private static final MultiTablesDataSyncJobGenerator INSTANCE = new MultiTablesDataSyncJobGenerator();

	private static final Map<String, DataSourceConverter> converters = MapUtils.newHashMap();

	private static volatile Map<String, SourceFactory<Source<Tuple2<String, Row>, ?, ?>>> factories = MapUtils
			.newHashMap();

	static {
		SourceFactory factory;
		ServiceLoader<SourceFactory> loader = ServiceLoader.load(SourceFactory.class);
		for (Iterator<SourceFactory> it = loader.iterator(); it.hasNext();) {
			factory = it.next();
			factories.put(factory.factoryIdentifier(), factory);
		}
	}

	private static final Pattern METADATA_PATTERN = Pattern.compile("METADATA[\\s]+FROM[\\s]+'[\\S]+'[\\s]+VIRTUAL");

	static {
		DataSourceConverter converter;
		ServiceLoader<DataSourceConverter> loader = ServiceLoader.load(DataSourceConverter.class);
		for (Iterator<DataSourceConverter> it = loader.iterator(); it.hasNext();) {
			converter = it.next();
			converters.put(converter.connector(), converter);
		}
	}

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
		SourceFactory<Source<Tuple2<String, Row>, ?, ?>> sourceFactory = getSourceFactory(connector);

		Map<String, Set<String>> tablePrimaryKeys = parseTableConfigs(dataSync.getPrimaryKey());
		Map<String, Set<String>> autoColumnses = parseTableConfigs(dataSync.getAutoColumns());
		String defaultAutoColumnsStr = getDefaultAutoColumns();
		Map<String, String> defaultAutoColumns = StringUtils.isBlank(defaultAutoColumnsStr) ? Collections.emptyMap()
				: toMap(TO_LOWERCASE, defaultAutoColumnsStr.split(AUTO_COLUMNS_SPLIT));
		String tables[] = dataSync.getTable().split(","), defaultDatabase = tableEnv.getCurrentDatabase(), fromType,
				toTable, parts[], sql;
		Map<String, String[]> actualPrimaryKeys = MapUtils.newHashMap();// 实际主键
		Map<String, List<Column>> columnses = MapUtils.newHashMap(tables.length);
		Map<String, RowTypeInfo> rowTypeInfos = MapUtils.newHashMap();
		Map<String, RowType> rowTypes = MapUtils.newHashMap();
		Map<String, Map<Integer, String>> metadatas = MapUtils.newHashMap();
		Map<Integer, String> metadata;
		for (int i = 0, columnIndex = 0; i < tables.length; i++, columnIndex = 0) {
			String table = tables[i].trim();
			Set<String> primaryKeys = new HashSet<String>(), autoColumns = new HashSet<String>(),
					pks = tablePrimaryKeys.get(table), atcs = autoColumnses.get(table);
			if (CollectionUtils.isEmpty(pks)) {
				pks = tablePrimaryKeys.get(null);// 获取全局主键配置
			}
			if (CollectionUtils.isNotEmpty(pks)) {
				primaryKeys.addAll(pks);
			}
			if (CollectionUtils.isEmpty(atcs)) {// 获取特定表的自动添加列配置
				atcs = autoColumnses.get(null);// 获取全局自动添加列配置
			}
			if (CollectionUtils.isNotEmpty(atcs)) {
				autoColumns.addAll(atcs);
			}
			List<Column> columns = collation(dataSync, params, sinkDataSource, table, primaryKeys,
					CollectionUtils.isEmpty(autoColumns) ? MapUtils.toHashMap(defaultAutoColumns)
							: toMap(TO_LOWERCASE, autoColumns));// 任务中没有自动添加列配置，则使用配置文件的配置
			LogicalType logicalType;
			List<String> fromNames = new ArrayList<String>();
			List<LogicalType> logicalTypes = new ArrayList<LogicalType>();
			List<TypeInformation<?>> typeInformations = new ArrayList<TypeInformation<?>>();
			metadata = MapUtils.newHashMap();
			for (Column column : columns) {
				if (!Strategy.TO.equals(column.getStrategy())) {// 跳过无来源的列，例如直接写入当前时间的列
					fromNames.add(decodeKeyword(column.getFromName()));
					fromType = column.getFromType();
					Matcher matcher = METADATA_PATTERN.matcher(fromType);
					if (matcher.find()) {
						int start = matcher.start(), end = matcher.end();
						if (end < fromType.length() - 1) {
							fromType = StringUtils.concat(fromType.substring(0, start), fromType.substring(end + 1));
						} else {
							fromType = fromType.substring(0, matcher.start());
						}
						String group = matcher.group();
						metadata.put(columnIndex, group.substring(group.indexOf(SQLUtils.SINGLE_QUOTATION_MARK) + 1,
								group.lastIndexOf(SQLUtils.SINGLE_QUOTATION_MARK)));
					}
					DataType dataType = DataTypeUtils.fromFlinkSQLType(fromType);
					logicalType = dataType.getLogicalType();
					logicalTypes.add(logicalType);
					typeInformations.add(InternalTypeInfo.of(logicalType));
					columnIndex++;
				}
			}
			columnses.put(table, columns);
			metadatas.put(table, metadata);

			parts = table.split("\\.", 2);
			if (parts.length > 1) {//
				toTable = parts[1];
				tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS ".concat(parts[0]));
				tableEnv.executeSql("USE ".concat(parts[0]));
			} else {
				tableEnv.executeSql("USE ".concat(defaultDatabase));
				toTable = table;
			}

			DataSourceConverter converter = converters.get(sinkDataSource.get("connector"));
			PrimaryKeysCollector collector = new PrimaryKeysCollector() {
				@Override
				public void collect(Set<String> primaryKeys) {
					actualPrimaryKeys.put(table, primaryKeys.toArray(new String[0]));
				}
			};
			if (converter == null) {
				sql = sinkTableSQL(sinkDataSource, toTable, columns, primaryKeys, params, collector);
			} else {
				sql = sinkTableSQL(converter.convert(sinkDataSource, table), toTable, columns, primaryKeys, params,
						collector);
			}
			if (log.isInfoEnabled()) {
				log.info("Create sink table by Flink SQL: " + SQLUtils.hiddePassword(sql));
			}
			tableEnv.executeSql(sql);
			int cols = logicalTypes.size();
			String[] names = fromNames.toArray(new String[cols]);
			rowTypes.put(table, RowType.of(logicalTypes.toArray(new LogicalType[cols]), names));
			rowTypeInfos.put(table, new RowTypeInfo(typeInformations.toArray(new TypeInformation[0]), names));
		}

		if (ConfigurationUtils.isKafka(sourceDataSource)) {
			String topic = dataSync.getTopic();
			if (topic != null) {
				sourceDataSource.put(TOPIC_KEY, topic);
			}
		}

		Source<Tuple2<String, Row>, ?, ?> source = sourceFactory.create(sourceDataSource, rowTypes, metadatas);
		SingleOutputStreamOperator<Tuple2<String, Row>> stream = env
				.fromSource(source, WatermarkStrategy.noWatermarks(), connector).disableChaining().name(connector);

		StatementSet statementSet = tableEnv.createStatementSet();
		// stream 转 Table，创建临时视图，插入sink表
		String primaryKeys[], fromTable, prefix = ClinkContext.getProperty("data.sync.from-table-prefix");
		SingleOutputStreamOperator<Row> dataStream;
		for (Map.Entry<String, RowTypeInfo> entry : rowTypeInfos.entrySet()) {
			String tableName = entry.getKey();
			parts = tableName.split("\\.", 2);
			if (parts.length > 1) {
				fromTable = StringUtils.concat(parts[0], ".", prefix, parts[1]);
			} else {
				fromTable = prefix + tableName;
			}
			dataStream = stream.flatMap(new MultiTablesRowsFilterFunction(tableName), entry.getValue());
			primaryKeys = actualPrimaryKeys.get(tableName);
			if (primaryKeys == null) {
				tableEnv.createTemporaryView(fromTable, tableEnv.fromChangelogStream(dataStream));
			} else {
				tableEnv.createTemporaryView(fromTable,
						tableEnv.fromChangelogStream(dataStream, Schema.newBuilder().primaryKey(primaryKeys).build()));
			}
			sql = insertSQL(tableName, fromTable, columnses.get(tableName), params);
			if (log.isInfoEnabled()) {
				log.info("Execute Flink SQL: " + SQLUtils.hiddePassword(sql));
			}
			statementSet.addInsertSql(sql);
		}
		return statementSet.execute();
	}

	/**
	 * 解析关于同步表的配置
	 * 
	 * @param config
	 *            配置内容
	 * @return 同步表的配置的查找表
	 */
	private Map<String, Set<String>> parseTableConfigs(String config) {
		Map<String, Set<String>> tableConfigs = MapUtils.newHashMap();
		if (StringUtils.isNotBlank(config)) {
			Set<String> configs;
			String table, columnName, columnNames[] = config.split(",");
			for (int i = 0; i < columnNames.length; i++) {
				columnName = columnNames[i].trim();
				int index = columnName.lastIndexOf(".");
				if (index > 0) {
					table = columnName.substring(0, index);
					columnName = columnName.substring(index + 1);
				} else {
					table = null;
				}
				configs = tableConfigs.get(table);
				if (configs == null) {
					configs = new LinkedHashSet<String>();
					tableConfigs.put(table, configs);
				}
				configs.add(columnName);
			}
		}
		return tableConfigs;
	}

	// 校对和整理列配置
	private static List<Column> collation(DataSync dataSync, Map<String, Object> params,
			Map<String, String> sinkDataSource, String table, Set<String> primaryKeys, Map<String, String> autoColumns)
			throws Exception {
		List<Column> columns = dataSync.getColumns();
		if (CollectionUtils.isEmpty(columns)) {
			columns = new ArrayList<>();
		} else {
			columns = ObjectUtils.clone(columns);
			Column column;
			String fromName, toName, prefix = table.concat(".");
			for (Iterator<Column> it = columns.iterator(); it.hasNext();) {
				column = it.next();
				fromName = column.getFromName();
				toName = column.getToName();
				if (belongTo(prefix, fromName) && belongTo(prefix, toName)) {
					column.setFromName(fromName.replace(prefix, ""));
					if (StringUtils.isNotBlank(toName)) {
						column.setToName(toName.replace(prefix, ""));
					}
				} else {
					it.remove();
				}
			}
		}
		Boolean smart = dataSync.getSmart();
		if (smart == null) {
			smart = Boolean.valueOf(ClinkContext.getProperty(ClinkContext.SMART_MODE_CONFIG_KEY));
		}
		if (Boolean.TRUE.equals(smart)) {// 智能模式，自动查询列名、数据类型
			MetaDataGetter metaDataGetter = MetaDataGetterFactory.getMetaDataGetter(sinkDataSource);
			TableMetaData tableMetaData = metaDataGetter.getTableMetaData(sinkDataSource, table);
			if (primaryKeys.isEmpty()) {
				Set<String> pks = tableMetaData.getPrimaryKeys();
				if (pks != null) {
					primaryKeys.addAll(pks);
				}
			}
			String connector = sinkDataSource.get("connector");
			Map<String, ColumnType> columnTypes = tableMetaData.getColumns();
			if (columns.isEmpty()) {// 没有用户自定义列
				addSmartLoadColumns(connector, columns, columnTypes, params, autoColumns);
			} else {// 有用户自定义列
				collationPartlyCustom(connector, columns, params, columnTypes, autoColumns);
			}
			String columnName, strategy;
			for (Iterator<String> it = autoColumns.values().iterator(); it.hasNext();) {
				columnName = it.next();
				Column column = new Column();
				column.setFromName(columnName);
				column.setToName(columnName);// 目标列名和来源列名相同
				columnName = TO_LOWERCASE ? columnName.toLowerCase() : columnName;// 不区分大小写，统一转为小写
				strategy = getDefaultStrategy(columnName);
				if (Strategy.FROM.equals(strategy)) {// 如果目标库元数据或者用户定义列中没有该自动添加的列，但策略是from，则仍然添加该列
					column.setStrategy(strategy);
					column.setFromType(getDefaultFromType(columnName));
					column.setToType(getDefaultToType(columnName));
					column.setScript(getDefaultScript(columnName));
					columns.add(column);
				}
			}
		} else if (columns.isEmpty()) {// 没有用户自定义列
			throw new IllegalJobConfigException(
					"At least one column must be configured in manual mode, or set the configuration '"
							+ ClinkContext.SMART_MODE_CONFIG_KEY
							+ "=true' to enable automatic column acquisition in smart mode");
		} else {// 全部是用户自定义列
			collationCustom(columns, params, autoColumns);
			String columnName;
			for (Iterator<String> it = autoColumns.values().iterator(); it.hasNext();) {
				columnName = it.next();
				Column column = new Column();
				column.setFromName(columnName);
				column.setToName(columnName);// 目标列名和来源列名相同
				columnName = TO_LOWERCASE ? columnName.toLowerCase() : columnName;// 不区分大小写，统一转为小写
				column.setStrategy(getDefaultStrategy(columnName));
				column.setFromType(getDefaultFromType(columnName));
				column.setToType(getDefaultToType(columnName));
				column.setScript(getDefaultScript(columnName));
				columns.add(column);
			}
		}
		return columns;
	}

	private static final Map<String, String> toMap(boolean toLowercase, Set<String> columnNames) {
		Map<String, String> map = MapUtils.newHashMap();
		if (!CollectionUtils.isEmpty(columnNames)) {
			String columnName;
			if (toLowercase) {
				for (Iterator<String> it = columnNames.iterator(); it.hasNext();) {
					columnName = it.next();
					map.put(columnName.toLowerCase(), columnName);
				}
			} else {
				for (Iterator<String> it = columnNames.iterator(); it.hasNext();) {
					columnName = it.next();
					map.put(columnName, columnName);
				}
			}
		}
		return map;
	}

	/**
	 * 判断列名是否属于某个表
	 * 
	 * @param prefix
	 *            前缀
	 * @param columnName
	 *            列名
	 * @return
	 */
	private static boolean belongTo(String prefix, String columnName) {
		return StringUtils.isBlank(columnName) || !columnName.contains(".") || columnName.startsWith(prefix);
	}

	private static SourceFactory<Source<Tuple2<String, Row>, ?, ?>> getSourceFactory(String connector) {
		SourceFactory<Source<Tuple2<String, Row>, ?, ?>> factory = factories.get(connector);
		if (factory == null) {
			throw new IllegalConfigurationException("Cannot find source factory for connector " + connector);
		}
		return factory;
	}

	private static String decodeKeyword(String columnName) {
		int index = columnName.indexOf(SQLUtils.RESERVED_KEYWORD_WRAP_SUFFIX), start = index + 1;
		if (start > 0 && start < columnName.length()) {
			int end = columnName.lastIndexOf(SQLUtils.RESERVED_KEYWORD_WRAP_SUFFIX);
			if (end > start) {
				columnName = columnName.substring(start, end);
			}
		}
		return columnName;
	}

}
