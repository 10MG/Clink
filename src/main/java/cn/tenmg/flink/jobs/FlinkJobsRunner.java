package cn.tenmg.flink.jobs;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.alibaba.fastjson.JSON;

import cn.tenmg.flink.jobs.model.Params;

/**
 * 基于SpringBoot封装的Flink应用入口虚基类
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 */
public abstract class FlinkJobsRunner {

	/**
	 * 获取默认服务名称
	 * 
	 * @return 返回默认服务名称
	 */
	protected abstract String getDefaultService();

	/**
	 * 获取默认运行模式
	 * 
	 * @return 返回默认运行模式
	 */
	protected abstract RuntimeExecutionMode getDefaultRuntimeMode();

	/**
	 * 根据服务名称获取流处理服务
	 * 
	 * @param serviceName
	 *            服务名称
	 * @return 返回处理服务
	 */
	protected abstract StreamService getStreamService(String serviceName);

	/**
	 * 运行应用
	 * 
	 * @param args
	 *            运行参数
	 * @throws Exception
	 *             发生异常
	 */
	public void run(String... args) throws Exception {
		Params params;
		String serviceName = null;
		if (args == null || args.length <= 0) {
			params = new Params();
		} else {
			params = JSON.parseObject(args[0], Params.class);
			serviceName = params.getServiceName();
		}
		if (serviceName == null) {
			serviceName = getDefaultService();
			params.setServiceName(serviceName);
		}
		if (params.getRuntimeMode() == null) {
			params.setRuntimeMode(getDefaultRuntimeMode());
		}
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		RuntimeExecutionMode mode = params.getRuntimeMode();
		if (RuntimeExecutionMode.BATCH.equals(mode)) {
			env.setRuntimeMode(RuntimeExecutionMode.BATCH);
		} else if (RuntimeExecutionMode.STREAMING.equals(mode)) {
			env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		}
		getStreamService(serviceName).run(env, params);
		env.execute();
	}

}
