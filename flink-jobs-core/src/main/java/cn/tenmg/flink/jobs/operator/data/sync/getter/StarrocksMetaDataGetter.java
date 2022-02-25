package cn.tenmg.flink.jobs.operator.data.sync.getter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;

import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.utils.JDBCUtils;

/**
 * StarRocks元数据获取器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.3
 */
public class StarrocksMetaDataGetter extends AbstractJDBCMetaDataGetter {

	@Override
	Connection getConnection(Map<String, String> dataSource) throws Exception {
		String driver = dataSource.get("driver"), url = dataSource.get("jdbc-url"),
				dadatabase = dataSource.get("database-name");
		if (StringUtils.isBlank(driver)) {
			driver = FlinkJobsContext.getDefaultJDBCDriver(JDBCUtils.getProduct(url));
		}
		if (StringUtils.isNotBlank(dadatabase)) {
			url += "/" + dadatabase;
		}
		Class.forName(driver);
		return DriverManager.getConnection(url, dataSource.get("username"), dataSource.get("password"));
	}

}
