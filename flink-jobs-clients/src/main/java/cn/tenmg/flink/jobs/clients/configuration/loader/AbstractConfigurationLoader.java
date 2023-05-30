package cn.tenmg.flink.jobs.clients.configuration.loader;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;

import cn.tenmg.flink.jobs.clients.configuration.ConfigurationLoader;
import cn.tenmg.flink.jobs.clients.exception.ConfigurationLoadException;
import cn.tenmg.flink.jobs.clients.utils.PlaceHolderUtils;

/**
 * 抽象配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public abstract class AbstractConfigurationLoader implements ConfigurationLoader {

	@Override
	public void load(Properties config) throws ConfigurationLoadException {
		replacePlaceHolder(config);
		loadConfig(config);
		replacePlaceHolder(config);
	}

	/**
	 * 加载配置内容到配置对象中
	 * 
	 * @param config
	 *            配置对象
	 * @throws ConfigurationLoadException
	 *             配置加载异常
	 */
	protected abstract void loadConfig(Properties config) throws ConfigurationLoadException;

	/**
	 * 替换配置值中的占位符
	 * 
	 * @param config
	 *            配置对象
	 */
	protected void replacePlaceHolder(Properties config) {
		Entry<Object, Object> entry;
		Object value;
		for (Iterator<Entry<Object, Object>> it = config.entrySet().iterator(); it.hasNext();) {
			entry = it.next();
			value = entry.getValue();
			if (value != null) {
				config.put(entry.getKey(), PlaceHolderUtils.replace(value.toString(), config));
			}
		}
	}

}
