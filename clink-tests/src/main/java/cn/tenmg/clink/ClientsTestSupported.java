package cn.tenmg.clink;

import cn.tenmg.clink.clients.StandaloneRestClusterClient;
import cn.tenmg.clink.config.model.Clink;

/**
 * 支持客户端进行测试的支持类
 * 
 * @author June wjzhao@aliyun.com
 * @since 2024年5月28日
 */
public class ClientsTestSupported extends XMLConfigTestSupported {

	protected static final StandaloneRestClusterClient client = new StandaloneRestClusterClient();

	protected static void test(String pathInClasspath) throws Exception {
		Clink clink = load(pathInClasspath);
		client.submit(clink);
	}
}
