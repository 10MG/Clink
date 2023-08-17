package cn.tenmg.clink.configuration.loader;

import java.io.IOException;
import java.util.Properties;

import cn.tenmg.clink.exception.ConfigurationLoadException;
import cn.tenmg.dsl.utils.PropertiesLoaderUtils;

/**
 * .properties 文件配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public class PropertiesFileConfigurationLoader extends AbstractConfigurationLoader {

	private static final String DEFAULT_CONFIG_LOCATION = "clink.properties";

	@Override
	protected void loadConfig(Properties config) throws ConfigurationLoadException {
		String pathInClassPath = config.getProperty("clink.configuration-file", DEFAULT_CONFIG_LOCATION);
		try {
			PropertiesLoaderUtils.load(config, pathInClassPath);
		} catch (IOException e) {
			throw new ConfigurationLoadException(
					"Unable to load configuration from " + pathInClassPath + " file in classpath");
		}
	}

}
