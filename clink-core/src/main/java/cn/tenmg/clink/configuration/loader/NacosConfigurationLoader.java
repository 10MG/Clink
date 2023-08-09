package cn.tenmg.clink.configuration.loader;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Properties;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;

import cn.tenmg.clink.exception.ConfigurationLoadException;
import cn.tenmg.clink.utils.ConfigurationUtils;

/**
 * Nacos 配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public class NacosConfigurationLoader extends PropertiesFileConfigurationLoader {

	private static final String NACOS_CONFIG_PREFIX = "nacos.config.";

	@Override
	public void load(Properties config) throws ConfigurationLoadException {
		super.load(config);
		loadNacosConfig(config, ConfigurationUtils.getPrefixedKeyValuePairs(config, NACOS_CONFIG_PREFIX, true));
		replacePlaceHolder(config);
	}

	protected void loadNacosConfig(Properties config, Properties nacos) throws ConfigurationLoadException {
		String group = nacos.getProperty("group"), dataIds[] = nacos.getProperty("dataIds").split(","), dataId = null;
		long timeoutMs = Long.valueOf(
				ConfigurationUtils.getProperty(nacos, Arrays.asList("pollTimeoutMs", "configLongPollTimeout"), "3000"));
		try {
			ConfigService configService = NacosFactory.createConfigService(nacos);
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
			throw new ConfigurationLoadException("Unable to get configuration from nacos wich dataId is " + dataId, e);
		}
	}

}
