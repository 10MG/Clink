package cn.tenmg.clink.jdbc.switcher;

import cn.tenmg.clink.jdbc.DatabaseSwitcher;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 受欢迎的 JDBC 数据库目录切换器。支持 mysql、postgresql、oceanbase、tidb、gbase
 * 这些产品，同时也是默认的数据库目录切换器。
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.6.0
 */
public class PopularDatabaseSwitcher implements DatabaseSwitcher {

	private static final String products[] = { "mysql", "postgresql", "oceanbase", "gbase" };

	@Override
	public String[] products() {
		return products;
	}

	@Override
	public String change(String url, String catalog) {
		int hostIndex = url.indexOf("://") + 3, catalogIndex = url.indexOf("/", hostIndex), paramsBegin;
		if (catalogIndex > 0) {
			paramsBegin = url.indexOf("?", catalogIndex);
			if (paramsBegin > 0) {
				return StringUtils.concat(url.substring(0, catalogIndex + 1), catalog, url.substring(paramsBegin));
			} else {
				return StringUtils.concat(url.substring(0, catalogIndex + 1), catalog);
			}
		} else {
			paramsBegin = url.indexOf("?", hostIndex);
			if (paramsBegin > 0) {
				return StringUtils.concat(url.substring(0, paramsBegin), "/", catalog, url.substring(paramsBegin));
			} else {
				return StringUtils.concat(url, "/", catalog);
			}
		}
	}

}
