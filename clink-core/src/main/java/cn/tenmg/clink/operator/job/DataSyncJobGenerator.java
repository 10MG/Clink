package cn.tenmg.clink.operator.job;

import java.util.Map;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import cn.tenmg.clink.model.DataSync;

/**
 * 数据同步作业生成器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public interface DataSyncJobGenerator {

	/**
	 * 生成数据同步任务
	 * 
	 * @param env
	 *            流执行环境
	 * @param dataSync
	 *            同步配置对象
	 * @param params
	 *            参数查找表
	 * @return 生成数据同步任务的结果
	 * @throws Exception
	 *             发生异常
	 */
	Object generate(StreamExecutionEnvironment env, DataSync dataSync, Map<String, Object> params) throws Exception;

}
