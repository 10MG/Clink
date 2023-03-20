package cn.tenmg.flink.jobs.datasource.filter;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import cn.tenmg.dsl.utils.MatchUtils;
import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.context.FlinkJobsContext;
import cn.tenmg.flink.jobs.datasource.DataSourceFilter;

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
			String properties[] = property.split(PROPERTIES_SPLITOR);
			for (Iterator<Entry<String, String>> it = dataSource.entrySet().iterator(); it.hasNext();) {
				if (MatchUtils.matchesAny(properties, it.next().getKey())) {
					it.remove();
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
