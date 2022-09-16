package cn.tenmg.flink.jobs.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.datasource.DatasourceBuilder;
import cn.tenmg.flink.jobs.exception.IllegalConfigurationException;

/**
 * JDBC 工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public abstract class JDBCUtils {

	private static final String JDBC_PRODUCT_SPLIT = ":", TYPE_NAME = "type",
			DEFAULT_TYPES[] = { "com.alibaba.druid.pool.DruidDataSource", "org.apache.commons.dbcp2.BasicDataSource" },
			BUILDER_PREFIX = "cn.tenmg.flink.jobs.datasource.builder.", BUILDER_SUFFIX = "Builder";

	private static final Map<String, DataSource> dataSources = new HashMap<String, DataSource>();

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
		String driver = dataSource.get("driver"), url = dataSource.get("url");
		if (StringUtils.isBlank(driver)) {
			driver = FlinkJobsContext.getDefaultJDBCDriver(getProduct(url));
		}
		Class.forName(driver);
		return DriverManager.getConnection(url, dataSource.get("username"), dataSource.get("password"));
	}

	/**
	 * 根据数据源名称获取JDBC数据库连接
	 * 
	 * @param name
	 *            数据源名称
	 * @return 返回JDBC数据库连接
	 * @throws SQLException
	 *             SQL异常
	 * @throws ClassNotFoundException
	 *             类未找到异常
	 */
	@SuppressWarnings("unchecked")
	public static final Connection getConnection(String name) throws SQLException, ClassNotFoundException {
		DataSource dataSource = dataSources.get(name);// 获取已缓存的数据源
		if (dataSource == null) {
			synchronized (dataSources) {
				dataSource = dataSources.get(name);
				if (dataSource == null) {
					Map<String, String> datasource = FlinkJobsContext.getDatasource(name);
					String type = datasource.get(TYPE_NAME), defaultType;
					if (StringUtils.isBlank(type)) {
						for (int i = 0; i < DEFAULT_TYPES.length; i++) {
							defaultType = DEFAULT_TYPES[i];
							if (isPresent(defaultType)) {
								type = defaultType;
								break;
							}
						}
					}
					if (StringUtils.isBlank(type)) {
						return getConnection(datasource);
					}
					String buildName = BUILDER_PREFIX.concat(type).concat(BUILDER_SUFFIX);
					try {
						Class<DatasourceBuilder> datasourceBuilder = (Class<DatasourceBuilder>) Class
								.forName(buildName);
						Properties properties = new Properties();
						properties.putAll(datasource);
						dataSource = datasourceBuilder.newInstance().createDataSource(properties);
						dataSources.put(name, dataSource);
					} catch (Throwable e) {
						throw new IllegalConfigurationException(
								"This type of datasource is not supported at the moment: " + type, e);
					}
				}
			}
		}
		return dataSource.getConnection();
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

	private static boolean isPresent(String className) {
		try {
			Class.forName(className, false, JDBCUtils.class.getClassLoader());
			return true;
		} catch (Throwable e) {
			return false;
		}
	}

}
