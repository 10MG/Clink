package cn.tenmg.clink;

import java.util.Map;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 操作执行器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public interface Operator {
	/**
	 * 
	 * 执行操作
	 * 
	 * @param env
	 *            运行环境
	 * @param config
	 *            操作配置JSON字符串
	 * @param params
	 *            参数查找表
	 * @throws Exception
	 *             发生异常
	 */
	void execute(StreamExecutionEnvironment env, String config, Map<String, Object> params) throws Exception;
}
