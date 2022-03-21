package cn.tenmg.flink.jobs.clients;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgram.Builder;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.tenmg.flink.jobs.clients.context.FlinkJobsClientsContext;
import cn.tenmg.flink.jobs.config.model.FlinkJobs;

/**
 * 独立群集REST客户端flink-jobs客户端。用于远程提交、监控和停止flink任务
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.0
 */
public class StandaloneRestClusterClient extends AbstractFlinkJobsClient<StandaloneClusterId> {

	private static final Logger log = LoggerFactory.getLogger(StandaloneRestClusterClient.class);

	private static final Queue<Configuration> configurations = new LinkedList<Configuration>();

	private static final int COUNT;

	static {
		Configuration configuration = ConfigurationUtils
				.createConfiguration(FlinkJobsClientsContext.getConfigProperties());
		String rpcServers = FlinkJobsClientsContext.getProperty("jobmanager.rpc.servers");
		if (StringUtils.isBlank(rpcServers)) {
			configurations.add(configuration);
		} else {
			Configuration config;
			String servers[] = rpcServers.split(","), server[];
			for (int i = 0; i < servers.length; i++) {
				config = configuration.clone();
				server = servers[i].split(":", 2);
				config.set(JobManagerOptions.ADDRESS, server[0].trim());
				if (server.length > 1) {
					config.set(JobManagerOptions.PORT, Integer.parseInt(server[1].trim()));
				} else if (!config.contains(JobManagerOptions.PORT)) {
					config.set(JobManagerOptions.PORT, 6123);
				}
				configurations.add(config);
			}
		}
		COUNT = configurations.size();
	}

	@Override
	public JobID submit(FlinkJobs flinkJobs) throws Exception {
		RestClusterClient<StandaloneClusterId> client = null;
		try {
			Map<String, String> options = flinkJobs.getOptions();
			String classpaths = FlinkJobsClientsContext.getProperty("classpaths"),
					parallelism = FlinkJobsClientsContext.getProperty("parallelism.default", "1");
			SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();
			if (options != null && !options.isEmpty()) {
				if (options.containsKey("classpaths")) {
					classpaths = options.get("classpaths");
				}
				if (options.containsKey("parallelism")) {
					parallelism = options.get("parallelism");
				}
				if (!flinkJobs.isAllwaysNewJob() && options.containsKey("fromSavepoint")) {
					String savepointPath = options.get("fromSavepoint");
					if (options.containsKey("allowNonRestoredState")) {
						savepointRestoreSettings = SavepointRestoreSettings.forPath(savepointPath,
								"true".equals(options.get("allowNonRestoredState")));
					} else {
						savepointRestoreSettings = SavepointRestoreSettings.forPath(savepointPath);
					}
				}
			}

			Configuration configuration = getConfiguration();
			Builder builder = PackagedProgram.newBuilder().setConfiguration(configuration)
					.setEntryPointClassName(getEntryPointClassName(flinkJobs)).setJarFile(new File(getJar(flinkJobs)))
					.setUserClassPaths(toURLs(classpaths)).setSavepointRestoreSettings(savepointRestoreSettings);

			String arguments = getArguments(flinkJobs);
			if (!isEmptyArguments(arguments)) {
				builder.setArguments(arguments);
			}
			JobGraph jobGraph = PackagedProgramUtils.createJobGraph(builder.build(), configuration,
					Integer.parseInt(parallelism),
					Boolean.valueOf(FlinkJobsClientsContext.getProperty("suppress.output", "false")));

			Properties customConf = toProperties(flinkJobs.getConfiguration());
			client = getRestClusterClient(configuration, customConf);
			for (int i = 0; i < COUNT; i++) {
				try {
					return client.submitJob(jobGraph).get();
				} catch (Exception e) {
					if (client != null) {
						client.close();
					}
					if (i < COUNT) {
						log.error("Try to submit job fail", e);
						client = getRestClusterClient(getConfiguration(), customConf);// try next
					} else {
						throw e;
					}
				}
			}
			return null;
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	@Override
	public Acknowledge cancel(JobID jobId) throws Exception {
		RestClusterClient<StandaloneClusterId> client = null;
		try {
			client = getRestClusterClient();
			return client.cancel(jobId).get();
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	@Override
	public Collection<JobStatusMessage> listJobs() throws Exception {
		RestClusterClient<StandaloneClusterId> client = null;
		try {
			client = getRestClusterClient();
			return client.listJobs().get();
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	public JobDetailsInfo getJobDetails(JobID jobId) throws Exception {
		RestClusterClient<StandaloneClusterId> client = null;
		try {
			client = getRestClusterClient();
			return client.getJobDetails(jobId).get();
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	@Override
	public JobStatus getJobStatus(JobID jobId) throws Exception {
		RestClusterClient<StandaloneClusterId> client = null;
		try {
			client = getRestClusterClient();
			return client.getJobStatus(jobId).get();
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	@Override
	public JobResult requestJobResult(JobID jobId) throws Exception {
		RestClusterClient<StandaloneClusterId> client = null;
		try {
			client = getRestClusterClient();
			return client.requestJobResult(jobId).get();
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	@Override
	public String stop(JobID jobId) throws Exception {
		RestClusterClient<StandaloneClusterId> client = null;
		try {
			client = getRestClusterClient();
			return client.stopWithSavepoint(jobId, false, FlinkJobsClientsContext.getProperty("state.savepoints.dir"))
					.get();
		} finally {
			if (client != null) {
				client.close();
			}
		}
	}

	@Override
	public RestClusterClient<StandaloneClusterId> getClusterClient() throws Exception {
		return newRestClusterClient(getConfiguration());
	}

	@Override
	public RestClusterClient<StandaloneClusterId> getClusterClient(Properties customConf) throws Exception {
		return getRestClusterClient(getConfiguration(), customConf);
	}

	@Override
	public RestClusterClient<StandaloneClusterId> getRestClusterClient() throws Exception {
		return getClusterClient();
	}

	@Override
	public RestClusterClient<StandaloneClusterId> getRestClusterClient(Properties customConf) throws Exception {
		return getClusterClient(customConf);
	}

	private static synchronized Configuration getConfiguration() {
		Configuration configuration = configurations.poll();
		configurations.add(configuration);
		return configuration;
	}

	private RestClusterClient<StandaloneClusterId> getRestClusterClient(Configuration configuration,
			Properties customConf) throws Exception {
		if (customConf != null) {
			configuration = configuration.clone();
			configuration.addAll(ConfigurationUtils.createConfiguration(customConf));
		}
		return newRestClusterClient(configuration);
	}

	private RestClusterClient<StandaloneClusterId> newRestClusterClient(Configuration configuration) throws Exception {
		return new RestClusterClient<StandaloneClusterId>(configuration, StandaloneClusterId.getInstance());
	}

	private static List<URL> toURLs(String classpaths) throws MalformedURLException {
		if (classpaths == null || "".equals(classpaths.trim())) {
			return Collections.emptyList();
		} else {
			String[] paths;
			if (classpaths.contains(";")) {
				paths = classpaths.split(";");
			} else {
				paths = classpaths.split(",");
			}
			List<URL> urls = new ArrayList<URL>();
			for (int i = 0; i < paths.length; i++) {
				urls.add(new URL(paths[i].trim()));
			}
			return urls;
		}
	}

	private static Properties toProperties(String configuration) throws IOException {
		Properties properties = null;
		if (configuration != null) {
			properties = new Properties();
			properties.load(new StringReader(configuration));
		}
		return properties;
	}

}