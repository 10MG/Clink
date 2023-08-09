package cn.tenmg.clink.jdbc.switcher;

import cn.tenmg.clink.jdbc.DatabaseSwitcher;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * Oracle 数据库切换器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.6.0
 */
public class OracleDatabaseSwitcher implements DatabaseSwitcher {

	private static final String products[] = { "oracle" };

	@Override
	public String[] products() {
		return products;
	}

	@Override
	public String change(String url, String catalog) {
		int hostIndex = url.indexOf("@//");
		if (hostIndex > 0) {
			hostIndex += 3;
			int serviceNameIndex = url.indexOf("/", hostIndex);
			if (serviceNameIndex > 0) {
				return StringUtils.concat(url.substring(0, serviceNameIndex + 1), catalog);
			} else {
				return StringUtils.concat(url, '/', catalog);
			}
		} else {
			int atIndex = url.indexOf("@");
			if (atIndex > 0) {
				int portIndex = url.indexOf(":", atIndex + 1);
				if (portIndex > 0) {
					int sidIndex = url.indexOf(":", ++portIndex);
					if (sidIndex > 0) {
						return StringUtils.concat(url.substring(0, sidIndex + 1), catalog);
					} else {
						if (StringUtils.isNumber(url.substring(portIndex))) {
							return StringUtils.concat(url, ':', catalog);
						} else {
							return StringUtils.concat(url.substring(0, portIndex), catalog);
						}
					}
				} else {
					return StringUtils.concat(url.substring(0, atIndex), '@', catalog);
				}
			} else {
				return url;
			}
		}
	}

}
