package cn.tenmg.flink.jobs.operator.data.sync.getter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import cn.tenmg.flink.jobs.operator.data.sync.ColumnsGetter;
import cn.tenmg.flink.jobs.utils.JDBCUtils;

/**
 * JDBC列获取器
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public abstract class JDBCColumnsGetter implements ColumnsGetter {

	/**
	 * 根据连接、表名获取列名对数据类型的查找表
	 * 
	 * @param con       连接
	 * @param tableName 表名
	 * @return
	 * @throws Exception 发生异常
	 */
	protected abstract Map<String, String> getColumns(Connection con, String tableName) throws Exception;

	@Override
	public Map<String, String> getColumns(Map<String, String> dataSource, String tableName) throws Exception {
		Connection con = null;
		try {
			JDBCUtils.loadDriver(dataSource);
			con = DriverManager.getConnection(dataSource.get("url"), dataSource.get("username"),
					dataSource.get("password"));// 获得数据库连接
			con.setAutoCommit(true);
			return getColumns(con, tableName);
		} catch (ClassNotFoundException | SQLException e) {
			throw e;
		} finally {
			JDBCUtils.close(con);
		}
	}

}