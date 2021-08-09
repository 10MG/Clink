package cn.tenmg.flink.jobs.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;

/**
 * JDBC 工具类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public abstract class JdbcUtils {

	private static final Logger log = LogManager.getLogger(JdbcUtils.class);

	private static final String JDBC_PRODUCT_NAME_SPLIT = ":";

	private JdbcUtils() {
	}

	/**
	 * 加载JDBC驱动程序
	 * 
	 * @param dataSource
	 *            数据源配置查找表
	 * @throws ClassNotFoundException
	 *             未找到驱动类异常
	 */
	public static final void loadDriver(Map<String, String> dataSource) throws ClassNotFoundException {
		String driver = dataSource.get("driver"), url = dataSource.get("url");
		if (StringUtils.isBlank(driver)) {
			String tmp = url.substring(url.indexOf(JDBC_PRODUCT_NAME_SPLIT) + 1);
			driver = FlinkJobsContext.getDefaultJDBCDriver(tmp.substring(0, tmp.indexOf(JDBC_PRODUCT_NAME_SPLIT)));
		}
		Class.forName(driver);
	}

	/**
	 * 关闭连接
	 * 
	 * @param con
	 *            连接
	 */
	public static void close(Connection con) {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException ex) {
				if (log.isErrorEnabled()) {
					log.error("Could not close JDBC Connection", ex);
				}
				ex.printStackTrace();
			} catch (Throwable ex) {
				if (log.isErrorEnabled()) {
					log.error("Unexpected exception on closing JDBC Connection", ex);
				}
				ex.printStackTrace();
			}
		}
	}

	/**
	 * 关闭声明
	 * 
	 * @param st
	 */
	public static void close(Statement st) {
		if (st != null) {
			try {
				st.close();
			} catch (SQLException ex) {
				if (log.isErrorEnabled()) {
					log.error("Could not close JDBC Statement", ex);
				}
				ex.printStackTrace();
			} catch (Throwable ex) {
				if (log.isErrorEnabled()) {
					log.error("Unexpected exception on closing JDBC Statement", ex);
				}
				ex.printStackTrace();
			}
		}
	}

	/**
	 * 关闭结果集
	 * 
	 * @param rs
	 *            结果集
	 */
	public static void close(ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException ex) {
				if (log.isErrorEnabled()) {
					log.error("Could not close JDBC ResultSet", ex);
				}
			} catch (Throwable ex) {
				if (log.isErrorEnabled()) {
					log.error("Unexpected exception on closing JDBC ResultSet", ex);
				}
			}
		}
	}
}
