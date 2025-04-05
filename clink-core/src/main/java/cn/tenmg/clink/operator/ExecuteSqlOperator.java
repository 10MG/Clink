package cn.tenmg.clink.operator;

import java.util.Map;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.clink.model.ExecuteSql;
import cn.tenmg.clink.utils.DataSourceFilterUtils;
import cn.tenmg.clink.utils.SQLUtils;
import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * SQL操作执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.1.0
 */
public class ExecuteSqlOperator extends AbstractSqlOperator<ExecuteSql> {

	private static Logger log = LoggerFactory.getLogger(ExecuteSqlOperator.class);

	@Override
	Object execute(StreamTableEnvironment tableEnv, ExecuteSql executeSql, Map<String, Object> params)
			throws Exception {
		NamedScript namedScript = DSLUtils.parse(executeSql.getScript(), params);
		String datasource = executeSql.getDataSource(), statement = namedScript.getScript();
		if (StringUtils.isNotBlank(datasource)) {
			statement = SQLUtils.wrapDataSource(SQLUtils.toSQL(namedScript), DataSourceFilterUtils
					.filter(executeSql.getDataSourceFilter(), ClinkContext.getDatasource(datasource)));
		} else {
			statement = SQLUtils.toSQL(namedScript);
		}
		if (log.isInfoEnabled()) {
			log.info("Execute Flink SQL: " + SQLUtils.hiddePassword(statement));
		}
		return tableEnv.executeSql(statement);
	}

}