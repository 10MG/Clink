package cn.tenmg.clink.datasource.converter;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import cn.tenmg.clink.datasource.DataSourceConverter;
import cn.tenmg.clink.exception.IllegalJobConfigException;
import cn.tenmg.clink.jdbc.DatabaseSwitcher;
import cn.tenmg.clink.utils.JDBCUtils;
import cn.tenmg.clink.utils.SQLUtils;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.MapUtils.MapBuilder;

/**
 * JDBC 数据源转换器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class JDBCDataSourceConverter implements DataSourceConverter {

	private static final String CONNECTOR = "jdbc";

	private static final Map<String, DatabaseSwitcher> switchers = MapUtils.newHashMap();

	static {
		DatabaseSwitcher switcher;
		ServiceLoader<DatabaseSwitcher> loader = ServiceLoader.load(DatabaseSwitcher.class);
		for (Iterator<DatabaseSwitcher> it = loader.iterator(); it.hasNext();) {
			switcher = it.next();
			for (String product : switcher.products()) {
				switchers.put(product, switcher);
			}
		}
	}

	@Override
	public String connector() {
		return CONNECTOR;
	}

	@Override
	public Map<String, String> convert(Map<String, String> dataSource, String table) {
		String parts[] = table.split("\\.", 2);
		MapBuilder<HashMap<String, String>, String, String> builder = MapUtils
				.newHashMapBuilder(dataSource.size() + 1, String.class, String.class).putAll(dataSource);
		if (parts.length > 1) {
			String url = dataSource.get("url");
			dataSource.put("url", getCatalogSwitcher(JDBCUtils.getProduct(url)).change(url, parts[0]));
			builder.put(SQLUtils.TABLE_NAME, parts[1]);
		} else {
			builder.put(SQLUtils.TABLE_NAME, table);
		}
		return builder.build();
	}

	private static DatabaseSwitcher getCatalogSwitcher(String product) {
		DatabaseSwitcher switcher = switchers.get(product);
		if (switcher == null) {
			throw new IllegalJobConfigException("Cannot find the catalog switcher for the JDBC product " + product);
		}
		return switcher;
	}

}
