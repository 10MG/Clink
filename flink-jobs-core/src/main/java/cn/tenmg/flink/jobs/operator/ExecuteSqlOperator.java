package cn.tenmg.flink.jobs.operator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.Map;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.tenmg.dsl.NamedScript;
import cn.tenmg.dsl.Script;
import cn.tenmg.dsl.parser.JDBCParamsParser;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.model.ExecuteSql;
import cn.tenmg.flink.jobs.utils.ConfigurationUtils;
import cn.tenmg.flink.jobs.utils.DataSourceFilterUtils;
import cn.tenmg.flink.jobs.utils.JDBCUtils;
import cn.tenmg.flink.jobs.utils.JSONUtils;
import cn.tenmg.flink.jobs.utils.SQLUtils;

/**
 * SQL操作执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.1.0
 */
public class ExecuteSqlOperator extends AbstractSqlOperator<ExecuteSql> {

	private static Logger log = LoggerFactory.getLogger(ExecuteSqlOperator.class);

	private static final String
	/**
	 * 删除语句正则表达式
	 */
	DELETE_CLAUSE_REGEX = "[\\s]*[D|d][E|e][L|l][E|e][T|t][E|e][\\s]+[F|f][R|r][O|o][M|m][\\s]+[\\S]+",

			/**
			 * 更新语句正则表达式
			 */
			UPDATE_CLAUSE_REGEX = "[\\s]*[U|u][P|p][D|d][A|a][T|t][E|e][\\s]+[\\S]+[\\s]+[S|s][E|e][T|t][\\s]+[\\S]+";

	@Override
	Object execute(StreamTableEnvironment tableEnv, ExecuteSql executeSql, Map<String, Object> params)
			throws Exception {
		NamedScript namedScript = DSLUtils.parse(executeSql.getScript(), params);
		String datasource = executeSql.getDataSource(), statement = namedScript.getScript();
		if (StringUtils.isNotBlank(datasource)) {
			Map<String, String> dataSource = DataSourceFilterUtils.filter(executeSql.getDataSourceFilter(),
					FlinkJobsContext.getDatasource(datasource));
			if (ConfigurationUtils.isJDBC(dataSource)
					&& (statement.matches(DELETE_CLAUSE_REGEX) || statement.matches(UPDATE_CLAUSE_REGEX))) {// DELETE/UPDATE语句，使用JDBC执行
				Script<List<Object>> script = DSLUtils.toScript(namedScript.getScript(), namedScript.getParams(),
						JDBCParamsParser.getInstance());
				statement = script.getValue();
				Connection con = null;
				PreparedStatement ps = null;
				try {
					con = JDBCUtils.getConnection(dataSource);// 获得数据库连接
					con.setAutoCommit(true);
					ps = con.prepareStatement(statement);
					List<Object> parameters = script.getParams();
					JDBCUtils.setParams(ps, parameters);
					if (log.isInfoEnabled()) {
						log.info(String.format("Execute JDBC SQL: %s; parameters: %s", statement,
								JSONUtils.toJSONString(parameters)));
					}
					return ps.executeUpdate();// 执行删除或更新
				} finally {
					JDBCUtils.close(ps);
					JDBCUtils.close(con);
				}
			} else {
				statement = SQLUtils.toSQL(namedScript);
			}
			statement = SQLUtils.wrapDataSource(statement, dataSource);
		} else {
			statement = SQLUtils.toSQL(namedScript);
		}
		if (log.isInfoEnabled()) {
			log.info("Execute Flink SQL: " + SQLUtils.hiddePassword(statement));
		}
		return tableEnv.executeSql(statement);
	}

}