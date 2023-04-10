package cn.tenmg.flink.jobs.configuration.loader;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;

import cn.tenmg.flink.jobs.exception.ConfigurationLoadException;
import cn.tenmg.flink.jobs.utils.ConfigurationUtils;

/**
 * Nacos 配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public class NacosConfigurationLoader extends PropertiesFileConfigurationLoader {

	@Override
	protected void loadConfig(Properties config) throws ConfigurationLoadException {
		super.loadConfig(config);
		loadNacosConfig(config, ConfigurationUtils.getPrefixedKeyValuePairs(config, "nacos.config."));
		replacePlaceHolder(config);
	}

	protected void loadNacosConfig(Properties config, Properties nacos) throws ConfigurationLoadException {
		String group = config.getProperty("group"), dataIds[] = config.getProperty("dataIds").split(","), dataId = null;
		long timeoutMs = Long.valueOf(config.getProperty("pollTimeoutMs", "3000"));
		try {
			ConfigService configService = NacosFactory.createConfigService(config);
			StringReader sr;
			for (int i = 0; i < dataIds.length; i++) {
				dataId = dataIds[i];
				String content = configService.getConfig(dataId, group, timeoutMs);
				try {
					sr = new StringReader(content);
					try {
						config.load(sr);
					} finally {
						sr.close();
						sr = null;
					}
				} catch (IOException e) {
					throw new ConfigurationLoadException("Unable to load configuration from content: " + content, e);
				}
			}
		} catch (NacosException e) {
			throw new ConfigurationLoadException("Unable to get configuration wich dataId is " + dataId + " from nacos",
					e);
		}
	}

}
