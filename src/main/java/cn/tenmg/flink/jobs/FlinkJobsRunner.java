package cn.tenmg.flink.jobs;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cn.tenmg.flink.jobs.model.Arguments;

/**
 * 模块化flink-jobs应用入口虚基类。可用于基于SpringBoot的CommandLineRunner封装的模块化的flink应用
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.0.0
 */
public abstract class FlinkJobsRunner extends BasicFlinkJobsRunner {

	/**
	 * 根据服务名称获取流处理服务
	 * 
	 * @param serviceName
	 *            服务名称
	 * @return 返回处理服务
	 */
	protected abstract StreamService getStreamService(String serviceName);

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
