package cn.tenmg.flink.jobs.clients;

import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgram.Builder;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.tenmg.flink.jobs.clients.utils.FlinkJobsClientsUtils;
import cn.tenmg.flink.jobs.clients.utils.Sets;
import cn.tenmg.flink.jobs.config.model.FlinkJobs;
import cn.tenmg.flink.jobs.config.model.Operate;

/**
 * 独立群集REST客户端flink-jobs客户端。用于远程提交、监控和停止flink任务
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.0
 */
public class StandaloneRestClusterClient extends AbstractFlinkJobsClient<RestClusterClient<StandaloneClusterId>> {

	private static Logger log = LoggerFactory.getLogger(StandaloneRestClusterClient.class);

	private static final Set<String> localOperates = Sets.as("Bsh", "Jdbc");

	private static final Actuator<JobID, JobGraph> submitJobActuator = new Actuator<JobID, JobGraph>() {
		@Override
		public JobID execute(RestClusterClient<StandaloneClusterId> client, JobGraph jobGraph) throws Exception {
			return client.submitJob(jobGraph).get();
		}
	};

	private static final Actuator<Acknowledge, JobID> cancelJobActuator = new Actuator<Acknowledge, JobID>() {
		@Override
		public Acknowledge execute(RestClusterClient<StandaloneClusterId> client, JobID jobId) throws Exception {
			return client.cancel(jobId).get();
		}
	};

	private static final Actuator<Collection<JobStatusMessage>, Void> listJobsActuator = new Actuator<Collection<JobStatusMessage>, Void>() {
		@Override
		public Collection<JobStatusMessage> execute(RestClusterClient<StandaloneClusterId> client, Void none)
				throws Exception {
			return client.listJobs().get();
		}
	};

	private static final Actuator<JobDetailsInfo, JobID> getJobDetailsActuator = new Actuator<JobDetailsInfo, JobID>() {
		@Override
		public JobDetailsInfo execute(RestClusterClient<StandaloneClusterId> client, JobID jobId) throws Exception {
			return client.getJobDetails(jobId).get();
		}
	};

	private static final Actuator<JobStatus, JobID> getJobStatusActuator = new Actuator<JobStatus, JobID>() {
		@Override
		public JobStatus execute(RestClusterClient<StandaloneClusterId> client, JobID jobId) throws Exception {
			return client.getJobStatus(jobId).get();
		}
	};

	private static final Actuator<JobResult, JobID> requestJobResultActuator = new Actuator<JobResult, JobID>() {
		@Override
		public JobResult execute(RestClusterClient<StandaloneClusterId> client, JobID jobId) throws Exception {
			return client.requestJobResult(jobId).get();
		}
	};

	private static final Actuator<String, JobStopParams> stopJobActuator = new Actuator<String, JobStopParams>() {
		@Override
		public String execute(RestClusterClient<StandaloneClusterId> client, JobStopParams jobStopParams)
				throws Exception {
			return FlinkJobsClientsUtils.stop(client, jobStopParams.jobId, jobStopParams.savepointsDir).get();
		}
	};

	private static class JobStopParams {

		private JobID jobId;

		private String savepointsDir;

		public JobStopParams(JobID jobId, String savepointsDir) {
			super();
			this.jobId = jobId;
			this.savepointsDir = savepointsDir;
		}
	}

	@Override
	public JobID submit(FlinkJobs flinkJobs) throws Exception {
		Map<String, String> options = flinkJobs.getOptions();
		String classpaths = properties.getProperty("classpaths"),
				parallelism = properties.getProperty("parallelism.default", "1");
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
				.setEntryPointClassName(getEntryPointClassName(flinkJobs)).setJarFile(getJar(flinkJobs))
				.setUserClassPaths(toURLs(classpaths)).setSavepointRestoreSettings(savepointRestoreSettings);

		String arguments = getArguments(flinkJobs);
		if (!isEmptyArguments(arguments)) {
			builder.setArguments(arguments);
		}
		boolean submit = true;
		if (flinkJobs.getServiceName() == null) {
			List<Operate> operates = flinkJobs.getOperates();
			if (operates != null && !operates.isEmpty()) {
				submit = false;
				for (int i = 0, size = operates.size(); i < size; i++) {
					if (!localOperates.contains(operates.get(i).getType())) {
						submit = true;
						break;
					}
				}
			}
		}
		PackagedProgram packagedProgram = null;
		try {
			packagedProgram = builder.build();
			if (submit) {
				JobGraph jobGraph = PackagedProgramUtils.createJobGraph(packagedProgram, configuration,
						Integer.parseInt(parallelism),
						Boolean.valueOf(properties.getProperty("suppress.output", "false")));
				Properties customConf = toProperties(flinkJobs.getConfiguration());
				return retry(submitJobActuator, jobGraph, configuration, customConf);
			} else {
				final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
				Thread.currentThread().setContextClassLoader(packagedProgram.getUserCodeClassLoader());
				try {
					packagedProgram.invokeInteractiveModeForExecution();
				} finally {
					Thread.currentThread().setContextClassLoader(contextClassLoader);
				}
			}
		} finally {
			if (packagedProgram != null) {
				packagedProgram.close();
				packagedProgram = null;
			}
		}
		return null;
	}

	@Override
	public Acknowledge cancel(JobID jobId) throws Exception {
		return retry(cancelJobActuator, jobId, getConfiguration(), null);
	}

	@Override
	public Collection<JobStatusMessage> listJobs() throws Exception {
		return retry(listJobsActuator, null, getConfiguration(), null);
	}

	public JobDetailsInfo getJobDetails(JobID jobId) throws Exception {
		return retry(getJobDetailsActuator, jobId, getConfiguration(), null);
	}

	@Override
	public JobStatus getJobStatus(JobID jobId) throws Exception {
		return retry(getJobStatusActuator, jobId, getConfiguration(), null);
	}

	@Override
	public JobResult requestJobResult(JobID jobId) throws Exception {
		return retry(requestJobResultActuator, jobId, getConfiguration(), null);
	}

	@Override
	public String stop(JobID jobId) throws Exception {
		return retry(stopJobActuator, new JobStopParams(jobId, properties.getProperty("state.savepoints.dir")),
				getConfiguration(), null);
	}

	@Override
	public RestClusterClient<StandaloneClusterId> getClusterClient() throws Exception {
		return newRestClusterClient(getConfiguration());
	}

	@Override
	public RestClusterClient<StandaloneClusterId> getClusterClient(Properties customConf) throws Exception {
		return getRestClusterClient(getConfiguration(), customConf);
	}

	private <R, T> R retry(Actuator<R, T> actuator, T params, Configuration configuration, Properties customConf)
			throws Exception {
		for (int i = 1, size = configurations.size(); i < size; i++) {
			try {
				return tryOnce(actuator, params, configuration, customConf);
			} catch (Exception e) {
				log.warn("The " + i + "th attempt failed, trying the " + (i + 1) + "th times");
			}

		}
		return tryOnce(actuator, params, configuration, customConf);// Try for the last time
	}

	private <R, T> R tryOnce(Actuator<R, T> actuator, T params, Configuration configuration, Properties customConf)
			throws Exception {
		RestClusterClient<StandaloneClusterId> client = null;
		try {
			client = getRestClusterClient(configuration, customConf);
			return actuator.execute(client, params);
		} finally {
			if (client != null) {
				client.close();
				client = null;
			}
		}
	}

	private interface Actuator<R, T> {
		R execute(RestClusterClient<StandaloneClusterId> client, T params) throws Exception;
	}

	private synchronized Configuration getConfiguration() {
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