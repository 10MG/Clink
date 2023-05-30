package cn.tenmg.flink.jobs.clients.configuration.loader;

import java.io.IOException;
import java.util.Properties;

import cn.tenmg.flink.jobs.clients.exception.ConfigurationLoadException;
import cn.tenmg.flink.jobs.clients.utils.PropertiesLoaderUtils;

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
		String pathInClassPath = config.getProperty("flink.jobs.clients.configuration-file");
		if (pathInClassPath == null) {
			throw new ConfigurationLoadException(
					"You must configure the value of 'flink.jobs.clients.configuration-file'");
		}
		try {
			PropertiesLoaderUtils.load(config, pathInClassPath);
		} catch (IOException e) {
			throw new ConfigurationLoadException(
					"Unable to load configuration from " + pathInClassPath + " file in classpath");
		}
	}

}
