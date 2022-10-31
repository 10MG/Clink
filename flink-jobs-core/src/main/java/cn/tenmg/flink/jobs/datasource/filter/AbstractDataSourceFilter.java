package cn.tenmg.flink.jobs.datasource.filter;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.datasource.DataSourceFilter;
import cn.tenmg.flink.jobs.utils.MatchUtils;

/**
 * 数据源过滤器虚基类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.0
 *
 */
public abstract class AbstractDataSourceFilter implements DataSourceFilter {

	protected static final String PROPERTIES_SPLITOR = ",", VERSION_IGNORE_CONNECTORS[] = { "hbase", "elasticsearch" };

	abstract String getKeyPrefix();

	@Override
	public void doFilter(Map<String, String> dataSource) {
		if (dataSource == null) {
			return;
		}
		String connector = dataSource.get("connector"), keyPrefix = getKeyPrefix(),
				property = FlinkJobsContext.getProperty(keyPrefix + connector);
		if (property == null) {
			property = getVersionIgnoreProperty(keyPrefix, connector);
		}
		if (StringUtils.isNotBlank(property)) {
			String key, properties[] = property.split(PROPERTIES_SPLITOR);
			Entry<String, String> entry;
			for (Iterator<Entry<String, String>> it = dataSource.entrySet().iterator(); it.hasNext();) {
				entry = it.next();
				key = entry.getKey();
				if (MatchUtils.matchesAny(properties, key)) {
					dataSource.remove(key);
				}
			}
		}
	}

	protected String getVersionIgnoreProperty(String keyPrefix, String connector) {
		String versionIgnoreConnector;
		for (int i = 0; i < VERSION_IGNORE_CONNECTORS.length; i++) {
			versionIgnoreConnector = VERSION_IGNORE_CONNECTORS[i];
			if (connector.startsWith(versionIgnoreConnector)) {
				return FlinkJobsContext.getProperty(keyPrefix + versionIgnoreConnector);
			}
		}
		return null;
	}

}
