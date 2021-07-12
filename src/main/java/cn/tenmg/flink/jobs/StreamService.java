package cn.tenmg.flink.jobs;

import java.io.Serializable;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cn.tenmg.flink.jobs.model.Arguments;

/**
 * 流处理服务
 * 
 * @author 赵伟均
 * 
 * @since 1.0.0
 */
public interface StreamService extends Serializable {

	/**
	 * 运行服务
	 * 
	 * @param env
	 *            运行环境
	 * @param arguments
	 *            运行参数
	 * @throws Exception
	 *             服务发生异常
	 */
	void run(StreamExecutionEnvironment env, Arguments arguments) throws Exception;

}
