package cn.tenmg.flink.jobs.configuration.loader;

import java.io.IOException;
import java.util.Properties;

import cn.tenmg.dsl.utils.PropertiesLoaderUtils;
import cn.tenmg.flink.jobs.exception.ConfigurationLoadException;

/**
 * .properties 文件配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public class PropertiesFileConfigurationLoader extends AbstractConfigurationLoader {

	private static final String CONFIG_LOCATION_KEY = "flink.jobs.configuration-file",
			DEFAULT_CONFIG_LOCATION = "flink-jobs.properties";

	@Override
	protected void loadConfig(Properties config) throws ConfigurationLoadException {
		String pathInClassPath = config.getProperty(CONFIG_LOCATION_KEY);
		if (pathInClassPath == null) {// 兼容老版本
			pathInClassPath = config.getProperty("config.location", DEFAULT_CONFIG_LOCATION);
		}
		try {
			PropertiesLoaderUtils.load(config, pathInClassPath);
		} catch (IOException e) {
			throw new ConfigurationLoadException(
					"Unable to load configuration from " + pathInClassPath + " file in classpath");
		}
	}

}
