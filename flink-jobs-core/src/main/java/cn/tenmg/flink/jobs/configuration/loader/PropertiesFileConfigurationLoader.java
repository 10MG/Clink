package cn.tenmg.flink.jobs.configuration.loader;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import cn.tenmg.dsl.utils.PropertiesLoaderUtils;
import cn.tenmg.flink.jobs.exception.ConfigurationLoadException;
import cn.tenmg.flink.jobs.utils.ConfigurationUtils;

/**
 * .properties 文件配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public class PropertiesFileConfigurationLoader extends AbstractConfigurationLoader {

	private static final String DEFAULT_CONFIG_LOCATION = "flink-jobs.properties";

	@Override
	protected void loadConfig(Properties config) throws ConfigurationLoadException {
		String pathInClassPath = ConfigurationUtils.getProperty(config,
				Arrays.asList("flink.jobs.configuration-file", "config.location"), DEFAULT_CONFIG_LOCATION);
		try {
			PropertiesLoaderUtils.load(config, pathInClassPath);
		} catch (IOException e) {
			throw new ConfigurationLoadException(
					"Unable to load configuration from " + pathInClassPath + " file in classpath");
		}
	}

}
