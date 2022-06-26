package cn.tenmg.flink.jobs.clients;

import java.util.Collection;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;

import com.alibaba.fastjson.JSON;

import cn.tenmg.flink.jobs.clients.utils.ClassUtils;
import cn.tenmg.flink.jobs.config.loader.XMLConfigLoader;
import cn.tenmg.flink.jobs.config.model.FlinkJobs;

/**
 * StandaloneRestClusterClient测试
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.0
 *
 */
public class StandaloneRestClusterClientTest {

	public static void main(String[] args) throws Exception {
		StandaloneRestClusterClient client = new StandaloneRestClusterClient();
		FlinkJobs flinkJobs = XMLConfigLoader.getInstance()
				.load(ClassUtils.getDefaultClassLoader().getResourceAsStream("WordCount.xml"));
		JobID jobId = client.submit(flinkJobs);
		System.out.println("Flink job launched: " + jobId.toHexString());// 启动flink-jobs作业
		Thread.sleep(80000);

		// JobID jobId = JobID.fromHexString(hexString);
		JobStatus jobStatus = client.getJobStatus(jobId);// 获取作业状态
		System.out.println("Job status: " + jobStatus);
		
		// 高级功能
		//RestClusterClient<StandaloneClusterId> restClusterClient = client.getClusterClient(customConf);
		RestClusterClient<StandaloneClusterId> restClusterClient = client.getClusterClient();
		JobDetailsInfo jobDetailsInfo = restClusterClient.getJobDetails(jobId).get();
		System.out.println("Job details info: " + JSON.toJSONString(jobDetailsInfo));
		JobResult jobResult = restClusterClient.requestJobResult(jobId).get();
		System.out.println("Job result: " + JSON.toJSONString(jobResult));
		Collection<JobStatusMessage> jobs = restClusterClient.listJobs().get();
		System.out.println("Jobs: " + JSON.toJSONString(jobs));

		System.out.println(
				"Flink job of jobId: " + jobId.toHexString() + " stopped, savepoint path: " + client.stop(jobId));// 停止flink-jobs作业
	}
}
