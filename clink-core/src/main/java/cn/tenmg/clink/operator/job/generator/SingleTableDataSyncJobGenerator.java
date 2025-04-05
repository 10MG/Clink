package cn.tenmg.clink.operator.job.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.clink.exception.IllegalJobConfigException;
import cn.tenmg.clink.metadata.MetaDataGetter;
import cn.tenmg.clink.metadata.MetaDataGetter.TableMetaData;
import cn.tenmg.clink.metadata.MetaDataGetter.TableMetaData.ColumnType;
import cn.tenmg.clink.metadata.MetaDataGetterFactory;
import cn.tenmg.clink.model.DataSync;
import cn.tenmg.clink.model.data.sync.Column;
import cn.tenmg.clink.source.SourceFactory;
import cn.tenmg.clink.utils.ConfigurationUtils;
import cn.tenmg.clink.utils.SQLUtils;
import cn.tenmg.clink.utils.SourceFactoryUtils;
import cn.tenmg.dsl.utils.CollectionUtils;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.ObjectUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 单表数据同步作业生成器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class SingleTableDataSyncJobGenerator extends AbstractDataSyncJobGenerator {

	private static final String FROM_TABLE_PREFIX_KEY = "data.sync.from-table-prefix";

	private static final SingleTableDataSyncJobGenerator INSTANCE = new SingleTableDataSyncJobGenerator();

	/**
	 * 扩展的元数据
	 */
	private static final Set<String> EXT_METADATA = CollectionUtils
			.asSet(ClinkContext.getProperty("data.sync.ext-metadata", "").split(","));

	private SingleTableDataSyncJobGenerator() {
	}

	public static SingleTableDataSyncJobGenerator getInstance() {
		return INSTANCE;
	}

	@Override
	public Object generate(StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, DataSync dataSync,
			Map<String, String> sourceDataSource, Map<String, String> sinkDataSource, Map<String, Object> params)
			throws Exception {
		TableInfo tableInfo = collation(dataSync, sinkDataSource, params);
		List<Column> columns = tableInfo.columns;
		String table = dataSync.getTable(), fromTable = ClinkContext.getProperty(FROM_TABLE_PREFIX_KEY) + table;
		SourceInfo sourceInfo = sourceInfo(sourceDataSource, dataSync.getTopic(), table, fromTable, columns,
				tableInfo.primaryKeys, params);

		String sql = sourceInfo.sql;
		if (sql == null) {// sql 为 null 说明需获取摄取时间元数据（METADATA FROM 'igs_ts' VIRTUAL），原生 Flink CDC 的
							// sql-connector 不支持，改用 FromSourceFactoryDataSyncJobGenerator 实现
			return cn.tenmg.clink.operator.job.generator.FromSourceFactoryDataSyncJobGenerator.generate(env, tableEnv,
					sourceInfo.sourceFactory, dataSync, sourceDataSource, sinkDataSource, params);
		}

		TableConfig tableConfig = tableEnv.getConfig();
		if (tableConfig != null) {
			Configuration configuration = tableConfig.getConfiguration();
			String pipelineName = configuration.get(PipelineOptions.NAME);
			if (StringUtils.isBlank(pipelineName)) {
				configuration.set(PipelineOptions.NAME,
						"data-sync" + ClinkContext.CONFIG_SPLITER + String.join(ClinkContext.CONFIG_SPLITER,
								String.join("-", dataSync.getFrom(), "to", dataSync.getTo()), table));
			}
		}

		if (log.isInfoEnabled()) {
			log.info("Create source table by Flink SQL: " + SQLUtils.hiddePassword(sql));
		}
		tableEnv.executeSql(sql);

		sql = sinkTableSQL(sinkDataSource, table, columns, tableInfo.primaryKeys, params);
		if (log.isInfoEnabled()) {
			log.info("Create sink table by Flink SQL: " + SQLUtils.hiddePassword(sql));
		}
		tableEnv.executeSql(sql);

		sql = insertSQL(table, fromTable, columns, params);
		if (log.isInfoEnabled()) {
			log.info("Execute Flink SQL: " + sql);
		}
		return tableEnv.executeSql(sql);
	}

	/**
	 * 校对和整理列配置并返回主键列（多个列之间使用“,”分隔）
	 * 
	 * @param dataSync       数据同步配置对象
	 * @param sinkDataSource 汇数据源
	 * @param params         参数查找表
	 * @return 返回含列及主键列属性的表信息对象
	 * @throws Exception 发生异常
	 */
	private static TableInfo collation(DataSync dataSync, Map<String, String> sinkDataSource,
			Map<String, Object> params) throws Exception {
		TableInfo tableInfo = new TableInfo();
		List<Column> columns = dataSync.getColumns();
		if (columns == null) {
			tableInfo.columns = columns = new ArrayList<Column>();
		} else {
			tableInfo.columns = columns = ObjectUtils.clone(columns);
		}
		Boolean smart = dataSync.getSmart();
		if (smart == null) {
			smart = Boolean.valueOf(ClinkContext.getProperty(ClinkContext.SMART_MODE_CONFIG_KEY));
		}
		String primaryKey = dataSync.getPrimaryKey(), autoColumnsStr = dataSync.getAutoColumns();
		if (StringUtils.isNotBlank(primaryKey)) {
			tableInfo.primaryKeys = new HashSet<String>();
			String[] columnNames = primaryKey.split(",");
			for (int i = 0; i < columnNames.length; i++) {
				tableInfo.primaryKeys.add(columnNames[i].trim());
			}
		}
		if (StringUtils.isBlank(autoColumnsStr)) {// 没有指定自动添加列名，使用配置的全局默认值，并根据目标表的实际情况确定是否添加自动添加列
			autoColumnsStr = getDefaultAutoColumns();
		}
		Map<String, String> autoColumns = StringUtils.isBlank(autoColumnsStr) ? Collections.emptyMap()
				: toMap(TO_LOWERCASE, autoColumnsStr.split(AUTO_COLUMNS_SPLITER));// 不区分大小写，统一转为小写
		if (Boolean.TRUE.equals(smart)) {// 智能模式，自动查询列名、数据类型
			MetaDataGetter metaDataGetter = MetaDataGetterFactory.getMetaDataGetter(sinkDataSource);
			TableMetaData tableMetaData = metaDataGetter.getTableMetaData(sinkDataSource, dataSync.getTable());
			if (primaryKey == null) {
				tableInfo.primaryKeys = tableMetaData.getPrimaryKeys();
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
		return tableInfo;
	}

	private static SourceInfo sourceInfo(Map<String, String> dataSource, String topic, String table, String fromTable,
			List<Column> columns, Set<String> primaryKeys, Map<String, Object> params) throws IOException {
		Set<String> actualPrimaryKeys = newSet(primaryKeys);
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("CREATE TABLE ").append(SQLUtils.wrapIfReservedKeywords(fromTable)).append("(");
		Column column;
		String toName;
		int i = 0, size = columns.size();
		String fromType;

		String connector = dataSource.get("connector"), metadataKey;
		while (i < size) {
			column = columns.get(i++);
			if (Strategy.TO.equals(column.getStrategy())) {
				toName = column.getToName();
				actualPrimaryKeys.remove(toName == null ? column.getFromName() : toName);
			} else {
				fromType = column.getFromType();
				metadataKey = metadataKey(fromType);
				if (EXT_METADATA.contains(metadataKey)) {
					SourceFactory<Source<Tuple2<String, Row>, ?, ?>> sourceFactory = SourceFactoryUtils
							.getSourceFactory(connector);
					if (sourceFactory != null && contains(sourceFactory.metadataKeys(), metadataKey)) {
						return new SourceInfo(sourceFactory);
					}
				}
				sqlBuffer.append(column.getFromName()).append(DSLUtils.BLANK_SPACE).append(fromType);
				break;
			}
		}
		while (i < size) {
			column = columns.get(i++);
			if (Strategy.TO.equals(column.getStrategy())) {
				toName = column.getToName();
				actualPrimaryKeys.remove(toName == null ? column.getFromName() : toName);
			} else {
				fromType = column.getFromType();
				metadataKey = metadataKey(fromType);
				if (EXT_METADATA.contains(metadataKey)) {
					SourceFactory<Source<Tuple2<String, Row>, ?, ?>> sourceFactory = SourceFactoryUtils
							.getSourceFactory(connector);
					if (sourceFactory != null && contains(sourceFactory.metadataKeys(), metadataKey)) {
						return new SourceInfo(sourceFactory);
					}
				}
				sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(column.getFromName())
						.append(DSLUtils.BLANK_SPACE).append(fromType);
			}
		}
		if (!actualPrimaryKeys.isEmpty()) {
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append("PRIMARY KEY (")
					.append(String.join(", ", actualPrimaryKeys)).append(") NOT ENFORCED");
		}
		sqlBuffer.append(") ").append("WITH (");
		if (ConfigurationUtils.isKafka(dataSource)) {
			if (!dataSource.containsKey(GROUP_ID_KEY)) {
				dataSource.put(GROUP_ID_KEY, ClinkContext.getProperty("data.sync.group-id-prefix") + table);// 设置properties.group.id
			}
			if (topic != null) {
				dataSource.put(TOPIC_KEY, topic);
			}
		}
		SQLUtils.appendDataSource(sqlBuffer, dataSource, table);
		sqlBuffer.append(")");
		return new SourceInfo(sqlBuffer.toString());
	}

	private static Set<String> newSet(Set<String> set) {
		Set<String> newSet = new HashSet<String>();
		if (set != null) {
			newSet.addAll(set);
		}
		return newSet;
	}

	private static String metadataKey(String type) {
		Matcher matcher = METADATA_PATTERN.matcher(type);
		if (matcher.find()) {
			String group = matcher.group(), key = group.substring(group.indexOf(SQLUtils.SINGLE_QUOTATION_MARK) + 1,
					group.lastIndexOf(SQLUtils.SINGLE_QUOTATION_MARK));
			return key;
		}
		return null;
	}

	private static boolean contains(Set<String> metadataKeys, String metadataKey) {
		if (CollectionUtils.isNotEmpty(metadataKeys) || metadataKey == null) {
			return false;
		}
		return metadataKeys.contains(metadataKey);
	}

	private static class TableInfo {

		private List<Column> columns;

		private Set<String> primaryKeys;

	}

	private static class SourceInfo {

		private String sql;

		private SourceFactory<Source<Tuple2<String, Row>, ?, ?>> sourceFactory;

		private SourceInfo(String sql) {
			super();
			this.sql = sql;
		}

		private SourceInfo(SourceFactory<Source<Tuple2<String, Row>, ?, ?>> sourceFactory) {
			super();
			this.sourceFactory = sourceFactory;
		}
	}
}
