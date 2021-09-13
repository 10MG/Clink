package cn.tenmg.flink.jobs.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置加载工具类。请使用cn.tenmg.dsl.utils.PropertiesLoaderUtils替换
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
@Deprecated
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
		InputStream is = classLoader.getResourceAsStream(pathInClassPath);
		Properties properties = new Properties();
		if (pathInClassPath.endsWith(XML_FILE_SUFFIX)) {
			properties.loadFromXML(is);
		} else {
			properties.load(is);
		}
		return properties;
	}
}
