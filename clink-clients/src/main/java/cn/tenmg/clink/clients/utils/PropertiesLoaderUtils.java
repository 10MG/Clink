package cn.tenmg.clink.clients.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置加载工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.0
 */
public abstract class PropertiesLoaderUtils {


	private static final String XML_FILE_SUFFIX = ".xml";

	/**
	 * 从classPath下加载配置文件
	 * 
	 * @param pathInClassPath
	 *            配置文件相对于classPath的路径
	 * @return 返回配置属性对象
	 * @throws IOException
	 *             I/O异常
	 */
	public static Properties load(String pathInClassPath) throws IOException {
		Properties properties = new Properties();
		load(properties, pathInClassPath);
		return properties;
	}

	/**
	 * 忽略异常从classPath下加载配置文件
	 * 
	 * @param pathInClassPath
	 *            配置文件相对于classPath的路径
	 * @return 返回配置属性对象
	 * @throws IOException
	 *             I/O异常
	 */
	public static Properties loadIgnoreException(String pathInClassPath) {
		Properties properties = new Properties();
		loadIgnoreException(properties, pathInClassPath);
		return properties;
	}

	/**
	 * 从classPath下加载配置文件
	 * 
	 * @param properties
	 *            属性对象
	 * @param pathInClassPath
	 *            配置文件相对于classPath的路径
	 * @throws IOException
	 *             I/O异常
	 */
	public static void load(Properties properties, String pathInClassPath) throws IOException {
		ClassLoader classLoader = ClassUtils.getDefaultClassLoader();
		InputStream is = classLoader.getResourceAsStream(pathInClassPath);
		if (is != null) {
			try {
				if (pathInClassPath.endsWith(XML_FILE_SUFFIX)) {
					properties.loadFromXML(is);
				} else {
					properties.load(is);
				}
			} finally {
				is.close();
				is = null;
			}
		}
	}

	/**
	 * 忽略异常从classPath下加载配置文件
	 * 
	 * @param properties
	 *            属性对象
	 * @param pathInClassPath
	 *            配置文件相对于classPath的路径
	 */
	public static void loadIgnoreException(Properties properties, String pathInClassPath) {
		ClassLoader classLoader = ClassUtils.getDefaultClassLoader();
		InputStream is = classLoader.getResourceAsStream(pathInClassPath);
		if (is != null) {
			try {
				if (pathInClassPath.endsWith(XML_FILE_SUFFIX)) {
					properties.loadFromXML(is);
				} else {
					properties.load(is);
				}
			} catch (Exception e) {
			} finally {
				try {
					is.close();
				} catch (Exception e) {
				}
				is = null;
			}
		}
	}

}
