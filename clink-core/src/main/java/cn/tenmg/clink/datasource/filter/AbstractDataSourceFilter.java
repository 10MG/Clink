package cn.tenmg.clink.datasource.filter;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import cn.tenmg.clink.context.ClinkContext;
import cn.tenmg.clink.datasource.DataSourceFilter;
import cn.tenmg.dsl.utils.MatchUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 抽象数据源过滤器
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
				property = ClinkContext.getProperty(keyPrefix + connector);
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
				return ClinkContext.getProperty(keyPrefix.concat(versionIgnoreConnector));
			}
		}
		return null;
	}

}
