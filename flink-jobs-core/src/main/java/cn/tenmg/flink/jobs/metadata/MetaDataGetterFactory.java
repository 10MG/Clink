package cn.tenmg.flink.jobs.metadata;

import java.util.HashMap;
import java.util.Map;

import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.exception.IllegalConfigurationException;

/**
 * 元数据获取器工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public abstract class MetaDataGetterFactory {

	@Deprecated
	private static final String COLUMNS_GETTER_KEY_PREFIX = "data.sync.metadata.getter"
			+ FlinkJobsContext.CONFIG_SPLITER;

	private static final String METADATA_GETTER_KEY_PREFIX = "metadata.getter" + FlinkJobsContext.CONFIG_SPLITER;

	private static volatile Map<String, MetaDataGetter> COLUMNS_GETTERS = new HashMap<String, MetaDataGetter>();

	/**
	 * 根据使用的数据源获取元数据获取器实例
	 * 
	 * @param dataSource
	 *            数据源
	 * @return 返回列获取器实例
	 */
	public static MetaDataGetter getMetaDataGetter(Map<String, String> dataSource) {
		String connector = dataSource.get("connector");
		MetaDataGetter columnsGetter = COLUMNS_GETTERS.get(connector);
		if (columnsGetter == null) {
			synchronized (COLUMNS_GETTERS) {
				columnsGetter = COLUMNS_GETTERS.get(connector);
				if (columnsGetter == null) {
					String key = METADATA_GETTER_KEY_PREFIX + connector, className = FlinkJobsContext.getProperty(key);
					if (className == null) {// 兼容老版本data.sync.metadata.getter.*
						className = FlinkJobsContext.getProperty(COLUMNS_GETTER_KEY_PREFIX + connector);
					}
					if (className == null) {
						throw new IllegalArgumentException("MetaDataGetter for connector '" + connector
								+ "' is not supported, Please consider manually implementing the interface "
								+ MetaDataGetter.class.getName() + " and specifying the configuration key '" + key
								+ "' to your own class name in the configuration file "
								+ FlinkJobsContext.getConfigurationFile());
					} else if (StringUtils.isBlank(className)) {
						throw new IllegalConfigurationException(
								"The configuration of key '" + key + "' must be not blank");
					} else {
						try {
							columnsGetter = (MetaDataGetter) Class.forName(className).newInstance();
						} catch (InstantiationException | IllegalAccessException e) {
							throw new IllegalArgumentException(
									"Cannot instantiate MetaDataGetter for connector '" + connector + "'", e);
						} catch (ClassNotFoundException e) {
							throw new IllegalArgumentException(
									"Wrong MetaDataGetter configuration for connector " + connector + "'", e);
						}
					}
					COLUMNS_GETTERS.put(connector, columnsGetter);
				}
			}
		}
		return columnsGetter;
	}
}
