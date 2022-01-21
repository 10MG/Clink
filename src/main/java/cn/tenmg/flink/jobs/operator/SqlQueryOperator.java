package cn.tenmg.flink.jobs.operator;

import java.util.Map;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.model.SqlQuery;
import cn.tenmg.flink.jobs.utils.SQLUtils;

/**
 * SQL操作执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.1.0
 */
public class SqlQueryOperator extends AbstractSqlOperator<SqlQuery> {

	private static Logger log = LoggerFactory.getLogger(SqlQueryOperator.class);

	@Override
	Object execute(StreamTableEnvironment tableEnv, SqlQuery sqlQuery, Map<String, Object> params) throws Exception {
		NamedScript namedScript = DSLUtils.parse(sqlQuery.getScript(), params);
		String saveAs = sqlQuery.getSaveAs(), statement = SQLUtils.toSQL(namedScript);

		log.info("Execute query by Flink SQL: " + statement);
		Table table = tableEnv.sqlQuery(statement);
		String defaultCatalog = FlinkJobsContext.getDefaultCatalog(tableEnv);
		if (!defaultCatalog.equals(tableEnv.getCurrentCatalog())) {
			tableEnv.useCatalog(defaultCatalog);
		}
		tableEnv.createTemporaryView(saveAs, table);
		return table;
	}

}
