package cn.tenmg.flink.jobs.clients;

import org.apache.flink.api.common.JobID;

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
		System.out
				.println("Flink job of jobId: " + jobId.toHexString() + " stopped, savepoint path: " + client.stop(jobId));// 停止flink-jobs作业
	}
}
