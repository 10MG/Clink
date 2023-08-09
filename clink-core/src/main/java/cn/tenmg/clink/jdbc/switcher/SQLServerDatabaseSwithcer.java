package cn.tenmg.clink.jdbc.switcher;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cn.tenmg.clink.jdbc.DatabaseSwitcher;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * SQLServer 数据库目录切换器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.6.0
 */
public class SQLServerDatabaseSwithcer implements DatabaseSwitcher {

	private static final String products[] = { "sqlserver" }, DATABASE_NAME_KEY = "databaseName",
			PROPERTY_SPLITOR = ";", KEY_VALUE_SPLITOR = "=";

	private static final Pattern DATABASE_NAME_PATTERN = Pattern.compile("[Dd]atabase[Nn]ame=[^;]+");

	@Override
	public String[] products() {
		return products;
	}

	@Override
	public String change(String url, String catalog) {
		Matcher matcher = DATABASE_NAME_PATTERN.matcher(url);
		if (matcher.find()) {
			StringBuffer sqlBuffer = new StringBuffer();
			String parts[] = matcher.group().split(KEY_VALUE_SPLITOR);
			matcher.appendReplacement(sqlBuffer, StringUtils.concat(parts[0], KEY_VALUE_SPLITOR, catalog));
			int end = matcher.end();
			if (end < url.length()) {
				sqlBuffer.append(url.substring(end));
			}
			return sqlBuffer.toString();
		} else if (url.endsWith(PROPERTY_SPLITOR)) {
			return StringUtils.concat(url, DATABASE_NAME_KEY, KEY_VALUE_SPLITOR, catalog, PROPERTY_SPLITOR);
		} else {
			return StringUtils.concat(url, PROPERTY_SPLITOR, DATABASE_NAME_KEY, KEY_VALUE_SPLITOR, catalog);
		}
	}

}
