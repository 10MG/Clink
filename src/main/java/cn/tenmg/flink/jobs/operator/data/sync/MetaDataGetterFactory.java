package cn.tenmg.flink.jobs.operator.data.sync;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import cn.tenmg.flink.jobs.context.FlinkJobsContext;

/**
 * 元数据获取器工厂
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public abstract class MetaDataGetterFactory {

	private static final String COLUMNS_GETTER_KEY_PREFIX = "data.sync.metadata.getter.";

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
					String className = FlinkJobsContext.getProperty(COLUMNS_GETTER_KEY_PREFIX + connector);
					if (className == null) {
						throw new IllegalArgumentException(
								"MetaDataGetter for connector '" + connector + "' is not supported");
					} else if (StringUtils.isBlank(className)) {
						throw new IllegalArgumentException(
								"Cannot find MetaDataGetter for connector '" + connector + "'");
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
