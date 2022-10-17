package cn.tenmg.flink.jobs.clients.utils;

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
	public static Properties loadFromClassPath(String pathInClassPath) throws IOException {
		ClassLoader classLoader = ClassUtils.getDefaultClassLoader();
		InputStream is = null;
		Properties properties = new Properties();
		try {
			is = classLoader.getResourceAsStream(pathInClassPath);
			if (pathInClassPath.endsWith(XML_FILE_SUFFIX)) {
				properties.loadFromXML(is);
			} else {
				properties.load(is);
			}
		} finally {
			if (is != null) {
				is.close();
				is = null;
			}
		}
		return properties;
	}
}
