package cn.tenmg.flink.jobs;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;

import cn.tenmg.flink.jobs.config.model.FlinkJobs;

/**
 * flink-jobs应用程序客户端
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.0
 */
public interface FlinkJobsClient {

	/**
	 * 提交flink-jobs应用程序
	 * 
	 * @param flinkJobs
	 *            flink-jobs配置对象
	 * @return flink作业标识
	 * @throws Exception
	 *             发生异常
	 */
	JobID submit(FlinkJobs flinkJobs) throws Exception;

	/**
	 * 获取flink作业状态
	 * 
	 * @param jobId
	 *            flink作业标识
	 * @return flink作业状态
	 * @throws Exception
	 *             发生异常
	 */
	JobStatus getJobStatus(JobID jobId) throws Exception;

	/**
	 * 停止flink-jobs应用程序
	 * 
	 * @param jobId
	 *            flink作业标识
	 * @return 保存点位置
	 * @throws Exception
	 *             发生异常
	 */
	String stop(JobID jobId) throws Exception;

}
