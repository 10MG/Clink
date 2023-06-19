package cn.tenmg.flink.jobs.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import cn.tenmg.dsl.utils.ClassUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;

/**
 * JDBC 工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public abstract class JDBCUtils {

	private static final String JDBC_PRODUCT_SPLIT = ":";

	private JDBCUtils() {
	}

	/**
	 * 根据连接地址获取产品名称
	 * 
	 * @param url
	 *            连接地址
	 * @return 返回产品名称
	 */
	public static final String getProduct(String url) {
		String tmp = url.substring(url.indexOf(JDBC_PRODUCT_SPLIT) + 1);
		return tmp.substring(0, tmp.indexOf(JDBC_PRODUCT_SPLIT));
	}

	/**
	 * 根据数据源配置获取JDBC数据库连接
	 * 
	 * @param dataSource
	 *            数据源配置
	 * @return 返回JDBC数据库连接
	 * @throws SQLException
	 *             SQL异常
	 * @throws ClassNotFoundException
	 *             类未找到异常
	 */
	public static final Connection getConnection(Map<String, String> dataSource)
			throws SQLException, ClassNotFoundException {
		String url = dataSource.get("url");
		if (StringUtils.isBlank(url)) {
			url = dataSource.get("jdbc-url");
		}
		String driver = dataSource.get("driver");
		if (StringUtils.isBlank(driver)) {
			driver = FlinkJobsContext.getDefaultJDBCDriver(getProduct(url));
		}
		ensureDriverLoaded(driver);
		Connection con = DriverManager.getConnection(url, dataSource.get("username"), dataSource.get("password"));
		String catalog = dataSource.get("database-name");
		if (StringUtils.isNotBlank(catalog)) {
			con.setCatalog(catalog);
		}
		return con;
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
				ex.printStackTrace();
			} catch (Throwable ex) {
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
				ex.printStackTrace();
			} catch (Throwable ex) {
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
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (Throwable e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 设置参数
	 * 
	 * @param ps
	 *            SQL声明对象
	 * @param params
	 *            查询参数
	 * @throws SQLException
	 */
	public static void setParams(PreparedStatement ps, List<Object> params) throws SQLException {
		if (params == null || params.isEmpty()) {
			return;
		}
		for (int i = 0, size = params.size(); i < size; i++) {
			ps.setObject(i + 1, params.get(i));
		}
	}

	/**
	 * 确保驱动类已被加载
	 * 
	 * @param driver
	 *            驱动类名
	 * @throws SQLException
	 */
	private static void ensureDriverLoaded(String driver) throws SQLException {
		Class<?> cls = null;
		try {
			cls = ClassUtils.getDefaultClassLoader().loadClass(driver);
		} catch (ClassNotFoundException e) {
			// skip
		}
		if (cls == null) {
			try {
				Class.forName(driver);
			} catch (ClassNotFoundException e) {
				throw new SQLException(e.getMessage(), e);
			}
		}
	}

}
