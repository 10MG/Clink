package cn.tenmg.clink.runner;

import cn.tenmg.clink.FlinkJobsRunner;
import cn.tenmg.clink.StreamService;

/**
 * 支持使用类名表示服务的简单Clink运行程序
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.2
 */
public class SimpleFlinkJobsRunner extends FlinkJobsRunner {

	private static final SimpleFlinkJobsRunner INSTANCE = new SimpleFlinkJobsRunner();

	public static SimpleFlinkJobsRunner getInstance() {
		return INSTANCE;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected StreamService getStreamService(String serviceName) throws Exception {
		return ((Class<StreamService>) Class.forName(serviceName)).getConstructor().newInstance();
	}

}
