package cn.tenmg.flink.jobs.operator.data.sync.getter;

import java.sql.Connection;
import java.util.Map;

/**
 * MySQL列获取器
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.2
 *
 */
public class MySQLColumnsGetter extends JDBCColumnsGetter {

	@Override
	protected Map<String, String> getColumns(Connection con, String tableName) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
