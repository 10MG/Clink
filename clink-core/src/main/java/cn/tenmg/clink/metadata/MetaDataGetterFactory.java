package cn.tenmg.clink.metadata;

import java.util.Map;

import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.clink.exception.IllegalConfigurationException;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 元数据获取器工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public abstract class MetaDataGetterFactory {

	private static final String METADATA_GETTER_KEY_PREFIX = "metadata.getter" + ClinkContext.CONFIG_SPLITER;

	private static volatile Map<String, MetaDataGetter> META_DATA_GETTERS = MapUtils.newHashMap();

	/**
	 * 根据使用的数据源获取元数据获取器实例
	 * 
	 * @param dataSource
	 *            数据源
	 * @return 返回列获取器实例
	 */
	public static MetaDataGetter getMetaDataGetter(Map<String, String> dataSource) {
		String connector = dataSource.get("connector");
		MetaDataGetter metaDataGetter = META_DATA_GETTERS.get(connector);
		if (metaDataGetter == null) {
			synchronized (META_DATA_GETTERS) {
				metaDataGetter = META_DATA_GETTERS.get(connector);
				if (metaDataGetter == null) {
					String key = METADATA_GETTER_KEY_PREFIX + connector, className = ClinkContext.getProperty(key);
					if (className == null) {
						throw new IllegalConfigurationException("MetaDataGetter for connector '" + connector
								+ "' is not supported, Please consider manually implementing the interface "
								+ MetaDataGetter.class.getName() + " and specifying the configuration key '" + key
								+ "' to your own class name in the configuration");
					} else if (StringUtils.isBlank(className)) {
						throw new IllegalConfigurationException(
								"The configuration of key '" + key + "' must be not blank");
					} else {
						try {
							metaDataGetter = (MetaDataGetter) Class.forName(className).getConstructor().newInstance();
						} catch (ClassNotFoundException e) {
							throw new IllegalConfigurationException(
									"Wrong MetaDataGetter configuration for connector " + connector + "'", e);
						} catch (Exception e) {
							throw new IllegalConfigurationException(
									"Cannot instantiate MetaDataGetter for connector '" + connector + "'", e);
						}
					}
					META_DATA_GETTERS.put(connector, metaDataGetter);
				}
			}
		}
		return metaDataGetter;
	}
}
