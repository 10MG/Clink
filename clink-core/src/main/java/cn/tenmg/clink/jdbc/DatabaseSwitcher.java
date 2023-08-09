package cn.tenmg.clink.jdbc;

/**
 * JDBC 数据库切换器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public interface DatabaseSwitcher {
	/**
	 * 获取适用的JDBC产品名称
	 * 
	 * @return 适用的JDBC产品名称
	 */
	String[] products();

	/**
	 * 切换数据库
	 * 
	 * @param url
	 *            连接地址
	 * @param catalog
	 *            新目录
	 * @return 访问新的数据库目录对应的连接地址
	 */
	String change(String url, String catalog);
}
