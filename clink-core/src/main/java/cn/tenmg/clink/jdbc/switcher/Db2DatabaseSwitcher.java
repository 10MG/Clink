package cn.tenmg.clink.jdbc.switcher;

import cn.tenmg.clink.jdbc.DatabaseSwitcher;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * DB2 数据库切换器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.6.0
 */
public class Db2DatabaseSwitcher implements DatabaseSwitcher {

	private static final String products[] = { "db2" };

	@Override
	public String[] products() {
		return products;
	}

	@Override
	public String change(String url, String catalog) {
		int hostIndex = url.indexOf("://") + 3, catalogIndex = url.indexOf("/", hostIndex);
		if (catalogIndex > 0) {
			int semicolonIndex2 = url.indexOf(":", catalogIndex);
			if (semicolonIndex2 > 0) {
				return StringUtils.concat(url.substring(0, catalogIndex + 1), catalog, url.substring(semicolonIndex2));
			} else {
				return StringUtils.concat(url.substring(0, catalogIndex + 1), catalog);
			}
		} else {
			int semicolon = url.indexOf(":", hostIndex);
			if (semicolon > 0) {
				int portIndex = semicolon + 1, semicolon2 = url.indexOf(":", portIndex);
				if (semicolon2 > 0) {
					return StringUtils.concat(url.substring(0, semicolon2), "/", catalog, url.substring(semicolon2));
				} else {
					if (StringUtils.isNumber(url.substring(portIndex))) {
						return StringUtils.concat(url, "/", catalog);
					} else {
						return StringUtils.concat(url.substring(0, semicolon), "/", catalog, url.substring(semicolon));
					}
				}
			} else {
				return StringUtils.concat(url, "/", catalog);
			}
		}
	}

}
