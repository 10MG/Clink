package cn.tenmg.clink;

import java.util.Collection;
import java.util.Properties;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;

import cn.tenmg.clink.config.model.Clink;

/**
 * Clink应用程序客户端
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.0
 */
public interface ClinkClient<T extends ClusterClient<?>> {

	/**
	 * 提交Clink应用程序
	 * 
	 * @param clink
	 *            Clink配置对象
	 * @return flink作业标识
	 * @throws Exception
	 *             发生异常
	 */
	JobID submit(Clink clink) throws Exception;

	/**
	 * 取消flink作业
	 * 
	 * @param jobId
	 *            flink作业标识
	 * @return 通用确认信息
	 * @throws Exception
	 *             发生异常
	 */
	Acknowledge cancel(JobID jobId) throws Exception;

	/**
	 * 列出集群上当前正在运行和已完成的flink作业
	 * 
	 * @return 正在运行和已完成的flink作业集
	 * @throws Exception
	 *             发生异常
	 */
	Collection<JobStatusMessage> listJobs() throws Exception;

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
	 * 请求flink作业运行结果
	 * 
	 * @param jobId
	 *            flink作业标识
	 * @return flink作业运行结果
	 * @throws Exception
	 *             发生异常
	 */
	JobResult requestJobResult(JobID jobId) throws Exception;

	/**
	 * 停止Clink应用程序
	 * 
	 * @param jobId
	 *            flink作业标识
	 * @return 保存点位置
	 * @throws Exception
	 *             发生异常
	 */
	String stop(JobID jobId) throws Exception;

	/**
	 * 使用默认配置获取flink集群REST客户端
	 * 
	 * @return 返回flink集群REST客户端
	 * @throws Exception
	 *             发生异常
	 */
	T getClusterClient() throws Exception;

	/**
	 * 使用自定义配置获取flink集群REST客户端
	 * 
	 * @param customConf
	 *            自定义配置
	 * @return 返回flink集群REST客户端
	 * @throws Exception
	 *             发生异常
	 */
	T getClusterClient(Properties customConf) throws Exception;

}
