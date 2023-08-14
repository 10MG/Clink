package cn.tenmg.clink.clients.configuration.loader;

import java.io.IOException;
import java.util.Properties;

import cn.tenmg.clink.clients.exception.ConfigurationLoadException;
import cn.tenmg.dsl.utils.PropertiesLoaderUtils;

/**
 * .properties 文件配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public class PropertiesFileConfigurationLoader extends AbstractConfigurationLoader {

	@Override
	protected void loadConfig(Properties config) throws ConfigurationLoadException {
		String pathInClassPath = config.getProperty("clink.clients.configuration-file");
		if (pathInClassPath == null) {
			throw new ConfigurationLoadException("You must configure the value of 'clink.clients.configuration-file'");
		}
		try {
			PropertiesLoaderUtils.load(config, pathInClassPath);
		} catch (IOException e) {
			throw new ConfigurationLoadException(
					"Unable to load configuration from " + pathInClassPath + " file in classpath");
		}
	}

}
