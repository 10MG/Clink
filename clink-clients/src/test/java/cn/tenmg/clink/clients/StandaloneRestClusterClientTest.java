package cn.tenmg.clink.clients;

import java.util.Collection;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;

import com.alibaba.fastjson.JSON;

import cn.tenmg.clink.clients.StandaloneRestClusterClient;
import cn.tenmg.clink.clients.utils.ClassUtils;
import cn.tenmg.clink.config.loader.XMLConfigLoader;
import cn.tenmg.clink.config.model.Clink;

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
		Clink clink = XMLConfigLoader.getInstance()
				.load(ClassUtils.getDefaultClassLoader().getResourceAsStream("WordCount.xml"));
		JobID jobId = client.submit(clink);
		System.out.println("Flink job launched: " + jobId.toHexString());// 启动Clink作业
		Thread.sleep(8000);

		// JobID jobId = JobID.fromHexString(hexString);
		JobStatus jobStatus = client.getJobStatus(jobId);// 获取作业状态
		System.out.println("Job status: " + jobStatus);
		
		// 高级功能
		//RestClusterClient<StandaloneClusterId> restClusterClient = client.getClusterClient(customConf);
		RestClusterClient<StandaloneClusterId> restClusterClient = client.getClusterClient();
		JobDetailsInfo jobDetailsInfo = restClusterClient.getJobDetails(jobId).get();
		System.out.println("Job details info: " + JSON.toJSONString(jobDetailsInfo));
		Collection<JobStatusMessage> jobs = restClusterClient.listJobs().get();
		System.out.println("Jobs: " + JSON.toJSONString(jobs));

		System.out.println(
				"Flink job of jobId: " + jobId.toHexString() + " stopped, savepoint path: " + client.stop(jobId));// 停止Clink作业

		JobResult jobResult = restClusterClient.requestJobResult(jobId).get();
		System.out.println("Job result: " + JSON.toJSONString(jobResult));
	}
}
