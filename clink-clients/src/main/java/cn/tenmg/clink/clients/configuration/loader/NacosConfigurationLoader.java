package cn.tenmg.clink.clients.configuration.loader;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Properties;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.exception.NacosException;

import cn.tenmg.clink.clients.exception.ConfigurationLoadException;
import cn.tenmg.clink.utils.ConfigurationUtils;

/**
 * Nacos 配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public class NacosConfigurationLoader extends AbstractConfigurationLoader {

	private static final String NACOS_CONFIG_PREFIX = "nacos.config.";

	@Override
	protected void loadConfig(Properties config) throws ConfigurationLoadException {
		Properties nacos = ConfigurationUtils.getPrefixedKeyValuePairs(config, NACOS_CONFIG_PREFIX, true);
		String group = nacos.getProperty("group"), dataIds[] = nacos.getProperty("dataIds").split(","), dataId = null;
		long timeoutMs = Long.valueOf(
				ConfigurationUtils.getProperty(nacos, Arrays.asList("pollTimeoutMs", "configLongPollTimeout"), "3000"));
		try {
			ConfigService configService = NacosFactory.createConfigService(nacos);
			StringReader sr;
			for (int i = 0; i < dataIds.length; i++) {
				dataId = dataIds[i];
				String content = configService.getConfig(dataId, group, timeoutMs);
				if (content == null) {
					throw new ConfigurationLoadException(String.format(
							"Unable to get configuration from nacos with namespace: %s, dataId: %s, group: %s.",
							nacos.getProperty("namespace"), dataId, group));
				}
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
			throw new ConfigurationLoadException(
					String.format("Unable to get configuration from nacos with namespace: %s, dataIds: %s, group: %s.",
							nacos.getProperty("namespace"), dataIds, group));
		}
	}
}
