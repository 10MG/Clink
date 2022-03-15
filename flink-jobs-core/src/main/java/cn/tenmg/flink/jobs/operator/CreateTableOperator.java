package cn.tenmg.flink.jobs.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.kit.HashMapKit;
import cn.tenmg.flink.jobs.model.CreateTable;
import cn.tenmg.flink.jobs.model.create.table.Column;
import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetter;
import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetter.TableMetaData;
import cn.tenmg.flink.jobs.operator.data.sync.MetaDataGetterFactory;
import cn.tenmg.flink.jobs.utils.SQLUtils;
import cn.tenmg.flink.jobs.utils.StreamTableEnvironmentUtils;

/**
 * 建表SQL操作执行器
 *
 * @author dufeng
 *
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.3.0
 *
 */
public class CreateTableOperator extends AbstractOperator<CreateTable> {

	private static Logger log = LoggerFactory.getLogger(CreateTableOperator.class);

	private static final String SMART_KEY = "data.sync.smart";

	@Override
	public Object execute(StreamExecutionEnvironment env, CreateTable createTable, Map<String, Object> params)
			throws Exception {
		String datasource = createTable.getDataSource(), tableName = createTable.getTableName();
		if (StringUtils.isBlank(datasource) || StringUtils.isBlank(tableName)) {
			throw new IllegalArgumentException("The property 'dataSource' or 'tableName' cannot be blank.");
		}
		StreamTableEnvironment tableEnv = FlinkJobsContext.getOrCreateStreamTableEnvironment(env);
		StreamTableEnvironmentUtils.useCatalogOrDefault(tableEnv, createTable.getCatalog());

		Map<String, String> dataSource = FlinkJobsContext.getDatasource(datasource);
		String primaryKey = collation(createTable, dataSource);
		String sql = createTableSQL(dataSource, tableName, createTable.getBindTableName(), createTable.getColumns(),
				primaryKey);
		if (log.isInfoEnabled()) {
			log.info("Create table by Flink SQL: " + SQLUtils.hiddePassword(sql));
		}
		return tableEnv.executeSql(sql);
	}

	/**
	 * 校对和整理列配置并返回主键列（多个列之间使用“,”分隔）
	 * 
	 * @param createTable
	 *            建表配置对象
	 * @param dataSource
	 *            数据源
	 * @return 返回主键
	 * @throws Exception
	 *             发生异常
	 */
	private static String collation(CreateTable createTable, Map<String, String> dataSource) throws Exception {
		List<Column> columns = createTable.getColumns();
		if (columns == null) {
			createTable.setColumns(columns = new ArrayList<Column>());
		}
		Boolean smart = createTable.getSmart();
		if (smart == null) {
			smart = Boolean.valueOf(FlinkJobsContext.getProperty(SMART_KEY));
		}
		String primaryKey = createTable.getPrimaryKey();
		if (Boolean.TRUE.equals(smart)) {// 智能模式，自动查询列名、数据类型
			MetaDataGetter metaDataGetter = MetaDataGetterFactory.getMetaDataGetter(dataSource);
			TableMetaData tableMetaData = metaDataGetter.getTableMetaData(dataSource, createTable.getTableName());
			Set<String> primaryKeys = tableMetaData.getPrimaryKeys();
			if (primaryKey == null && primaryKeys != null && !primaryKeys.isEmpty()) {
				primaryKey = String.join(",", primaryKeys);
			}

			if (!columns.isEmpty()) {// 有用户自定义列
				collationCustom(columns);
			}
			addSmartLoadColumns(columns, tableMetaData.getColumns());
		} else if (columns.isEmpty()) {// 没有用户自定义列
			throw new IllegalArgumentException(
					"At least one column must be configured in manual mode, or set the configuration '" + SMART_KEY
							+ "=true' at " + FlinkJobsContext.getConfigurationFile()
							+ " to enable automatic column acquisition in smart mode");
		} else {// 全部是用户自定义列
			collationCustom(columns);
		}
		return primaryKey;
	}

	/**
	 * 添加智能加载的列
	 * 
	 * @param columns
	 *            列的列表
	 * @param columnsMap
	 *            智能加载的列
	 */
	private static void addSmartLoadColumns(List<Column> columns, Map<String, String> columnsMap) {
		for (Iterator<Entry<String, String>> it = columnsMap.entrySet().iterator(); it.hasNext();) {
			Entry<String, String> entry = it.next();
			Column column = new Column();
			column.setName(SQLUtils.wrapIfReservedKeywords(entry.getKey()));// SQL保留关键字包装
			column.setType(entry.getValue());
			columns.add(column);
		}
	}

	/**
	 * 整理自定义列
	 * 
	 * @param columns
	 *            自定义列
	 */
	private static void collationCustom(List<Column> columns) {
		for (int i = 0, size = columns.size(); i < size; i++) {
			Column column = columns.get(i);
			column.setName(SQLUtils.wrapIfReservedKeywords(column.getName()));// SQL保留关键字包装
		}
	}

	private static String createTableSQL(Map<String, String> dataSource, String tableName, String bindTableName,
			List<Column> columns, String primaryKey) throws IOException {
		StringBuffer sqlBuffer = new StringBuffer();
		sqlBuffer.append("CREATE TABLE ").append(tableName).append("(");

		Column column = columns.get(0);
		sqlBuffer.append(column.getName()).append(DSLUtils.BLANK_SPACE).append(column.getType());

		for (int i = 1, size = columns.size(); i < size; i++) {
			column = columns.get(i);
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append(column.getName())
					.append(DSLUtils.BLANK_SPACE).append(column.getType());
		}
		if (StringUtils.isNotBlank(primaryKey)) {
			sqlBuffer.append(DSLUtils.COMMA).append(DSLUtils.BLANK_SPACE).append("PRIMARY KEY (").append(primaryKey)
					.append(") NOT ENFORCED");
		}
		sqlBuffer.append(") ").append("WITH (");
		SQLUtils.appendDataSource(sqlBuffer, HashMapKit.init(dataSource)
				.put(SQLUtils.TABLE_NAME, StringUtils.isBlank(bindTableName) ? tableName : bindTableName).get());
		sqlBuffer.append(")");
		return sqlBuffer.toString();
	}

}
