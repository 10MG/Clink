package cn.tenmg.flink.jobs;

import java.io.Serializable;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cn.tenmg.flink.jobs.model.Params;

/**
 * 流处理服务
 * 
 * @author 赵伟均
 *
 */
public interface StreamService extends Serializable {

	/**
	 * 运行服务
	 * 
	 * @param env
	 *            运行环境
	 * @param params
	 *            运行参数
	 * @throws Exception
	 *             服务发生异常
	 */
	void run(final StreamExecutionEnvironment env, Params params) throws Exception;

}
