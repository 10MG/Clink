package cn.tenmg.flink.jobs.clients.context;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.tenmg.flink.jobs.clients.utils.PropertiesLoaderUtils;

/**
 * flink-jobs-clients上下文
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.0
 */
public abstract class FlinkJobsClientsContext {

	private static final Logger log = LoggerFactory.getLogger(FlinkJobsClientsContext.class);

	private static final String DEFAULT_STRATEGIES_PATH = "flink-jobs-clients-context-loader.properties",
			CONFIG_LOCATION_KEY = "config.location";

	private static Properties configProperties;

	static {
		try {
			configProperties = PropertiesLoaderUtils.loadFromClassPath(DEFAULT_STRATEGIES_PATH);
		} catch (Exception e) {
			log.warn(DEFAULT_STRATEGIES_PATH + " not found in the classpath.", e);
			configProperties = new Properties();
		}
		String configurationFile = configProperties.getProperty(CONFIG_LOCATION_KEY, "flink-jobs-clients.properties");
		try {
			configProperties.putAll(PropertiesLoaderUtils.loadFromClassPath(configurationFile));
		} catch (Exception e) {
			log.info("Configuration file " + configurationFile
					+ " not found in classpath, the default configuration will be used.");
		}
	}

	/**
	 * 获取配置文件所在位置
	 * 
	 * @return 配置文件所在位置
	 */
	public static String getConfigLocation() {
		return getProperty(FlinkJobsClientsContext.CONFIG_LOCATION_KEY);
	}

	/**
	 * 获取用户配置属性
	 * 
	 * @return 用户配置属性
	 */
	public static Properties getConfigProperties() {
		return configProperties;
	}

	/**
	 * 根据键获取配置的属性
	 * 
	 * @param key
	 *            键
	 * @return 配置属性值或null
	 */
	public static String getProperty(String key) {
		return configProperties.getProperty(key);
	}

	/**
	 * 根据键获取配置的属性。如果配置属性不存在，则返回默认值
	 * 
	 * @param key
	 *            键
	 * @param defaultValue
	 *            默认值
	 * @return 配置属性值或默认值
	 */
	public static String getProperty(String key, String defaultValue) {
		return configProperties.containsKey(key) ? configProperties.getProperty(key) : defaultValue;
	}

}
