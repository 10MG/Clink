package cn.tenmg.clink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cn.tenmg.clink.model.Arguments;

/**
 * 支持服务的Clink运行程序。可用于封装的模块化的flink应用
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.0.0
 */
public abstract class ClinkRunner extends BasicClinkRunner {

	/**
	 * 根据服务名称获取流处理服务
	 * 
	 * @param serviceName
	 *            服务名称
	 * @return 返回处理服务
	 */
	protected abstract StreamService getStreamService(String serviceName) throws Exception;

	@Override
	protected void run(StreamExecutionEnvironment env, Arguments arguments) throws Exception {
		// 获取和运行服务
		String serviceName = arguments.getServiceName();
		if (serviceName != null) {
			StreamService streamService = getStreamService(serviceName);
			if (streamService != null) {
				streamService.run(env, arguments);
				env.execute(serviceName);
			}
		}
	}

}
