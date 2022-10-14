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
public class StandaloneRestClusterClient extends AbstractFlinkJobsClient<StandaloneClusterId> {

	private static Logger log = LoggerFactory.getLogger(StandaloneRestClusterClient.class);

	private static final Queue<Configuration> configurations = new LinkedList<Configuration>();

	private static final int COUNT;

	private static final Set<String> localOperates = Sets.as("Bsh", "Jdbc");

	static {
		Configuration configuration = ConfigurationUtils
				.createConfiguration(FlinkJobsClientsContext.getConfigProperties());
		String rpcServers = FlinkJobsClientsContext.getProperty("jobmanager.rpc.servers");
		if (isBlank(rpcServers)) {
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
		boolean submit = true;
		if (flinkJobs.getServiceName() == null) {
			submit = false;
			List<Operate> operates = flinkJobs.getOperates();
			if (operates != null) {
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
						Boolean.valueOf(FlinkJobsClientsContext.getProperty("suppress.output", "false")));
				Properties customConf = toProperties(flinkJobs.getConfiguration());
				return retry(new Actuator<JobID>() {
					@Override
					public JobID execute(RestClusterClient<StandaloneClusterId> client) throws Exception {
						return client.submitJob(jobGraph).get();
					}
				}, configuration, customConf);
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
		return retry(new Actuator<Acknowledge>() {
			@Override
			public Acknowledge execute(RestClusterClient<StandaloneClusterId> client) throws Exception {
				return client.cancel(jobId).get();
			}
		}, getConfiguration(), null);
	}

	@Override
	public Collection<JobStatusMessage> listJobs() throws Exception {
		return retry(new Actuator<Collection<JobStatusMessage>>() {
			@Override
			public Collection<JobStatusMessage> execute(RestClusterClient<StandaloneClusterId> client)
					throws Exception {
				return client.listJobs().get();
			}
		}, getConfiguration(), null);
	}

	public JobDetailsInfo getJobDetails(JobID jobId) throws Exception {
		return retry(new Actuator<JobDetailsInfo>() {
			@Override
			public JobDetailsInfo execute(RestClusterClient<StandaloneClusterId> client) throws Exception {
				return client.getJobDetails(jobId).get();
			}
		}, getConfiguration(), null);
	}

	@Override
	public JobStatus getJobStatus(JobID jobId) throws Exception {
		return retry(new Actuator<JobStatus>() {
			@Override
			public JobStatus execute(RestClusterClient<StandaloneClusterId> client) throws Exception {
				return client.getJobStatus(jobId).get();
			}
		}, getConfiguration(), null);
	}

	@Override
	public JobResult requestJobResult(JobID jobId) throws Exception {
		return retry(new Actuator<JobResult>() {
			@Override
			public JobResult execute(RestClusterClient<StandaloneClusterId> client) throws Exception {
				return client.requestJobResult(jobId).get();
			}
		}, getConfiguration(), null);
	}

	@Override
	public String stop(JobID jobId) throws Exception {
		return retry(new Actuator<String>() {
			@Override
			public String execute(RestClusterClient<StandaloneClusterId> client) throws Exception {
				return FlinkJobsClientsUtils.stop(client, jobId).get();
			}
		}, getConfiguration(), null);
	}

	@Override
	public RestClusterClient<StandaloneClusterId> getClusterClient() throws Exception {
		return newRestClusterClient(getConfiguration());
	}

	@Override
	public RestClusterClient<StandaloneClusterId> getClusterClient(Properties customConf) throws Exception {
		return getRestClusterClient(getConfiguration(), customConf);
	}

	private <T> T retry(Actuator<T> actuator, Configuration configuration, Properties customConf) throws Exception {
		for (int i = 1; i < COUNT; i++) {
			try {
				return tryOnce(actuator, configuration, customConf);
			} catch (Exception e) {
				log.warn("The " + i + "th attempt failed, trying the " + (i + 1) + "th times");
			}

		}
		return tryOnce(actuator, configuration, customConf);// Try for the last time
	}

	private <T> T tryOnce(Actuator<T> actuator, Configuration configuration, Properties customConf) throws Exception {
		RestClusterClient<StandaloneClusterId> client = null;
		try {
			client = getRestClusterClient(configuration, customConf);
			return actuator.execute(client);
		} finally {
			if (client != null) {
				client.close();
				client = null;
			}
		}
	}

	private interface Actuator<T> {
		T execute(RestClusterClient<StandaloneClusterId> client) throws Exception;
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

	/**
	 * 判断指定字符串是否为空（<code>null</code>）、空字符串（<code>""</code>）或者仅含空格的字符串
	 * 
	 * @param string
	 *            指定字符串
	 * @return 指定字符串为空（<code>null</code>）、空字符串（<code>""</code>）或者仅含空格的字符串返回
	 *         <code>true</code>，否则返回<code>false</code>
	 */
	private static boolean isBlank(String string) {
		int len;
		if (string == null || (len = string.length()) == 0) {
			return true;
		}
		for (int i = 0; i < len; i++) {
			if ((Character.isWhitespace(string.charAt(i)) == false)) {
				return false;
			}
		}
		return true;
	}

}