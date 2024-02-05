package cn.tenmg.flink.jobs.clients;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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

	private static final char SINGLE_QUOTATION_MARK = '\'', BACKSLASH = '\\', BLANK_SPACE = '\u0020', VALUE_BEGIN = '=';

	private static final Set<Character> VALUE_END = new HashSet<Character>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 6149301286530143148L;

		{
			add(',');
			add('\r');
			add('\n');
		}
	};

	private static Logger log = LoggerFactory.getLogger(StandaloneRestClusterClient.class);

	private static final Set<String> LOCAL_OPERATES = Sets.as("Bsh", "Jdbc");

	private static final Actuator<JobID, JobGraph> SUBMIT_JOB_ACTUATOR = new Actuator<JobID, JobGraph>() {
		@Override
		public JobID execute(RestClusterClient<StandaloneClusterId> client, JobGraph jobGraph) throws Exception {
			return client.submitJob(jobGraph).get();
		}
	};

	private static final Actuator<Acknowledge, JobID> CANCEL_JOB_ACTUATOR = new Actuator<Acknowledge, JobID>() {
		@Override
		public Acknowledge execute(RestClusterClient<StandaloneClusterId> client, JobID jobId) throws Exception {
			return client.cancel(jobId).get();
		}
	};

	private static final Actuator<Collection<JobStatusMessage>, Void> LIST_JOBS_ACTUATOR = new Actuator<Collection<JobStatusMessage>, Void>() {
		@Override
		public Collection<JobStatusMessage> execute(RestClusterClient<StandaloneClusterId> client, Void none)
				throws Exception {
			return client.listJobs().get();
		}
	};

	private static final Actuator<JobDetailsInfo, JobID> GET_JOB_DETAILS_ACTUATOR = new Actuator<JobDetailsInfo, JobID>() {
		@Override
		public JobDetailsInfo execute(RestClusterClient<StandaloneClusterId> client, JobID jobId) throws Exception {
			return client.getJobDetails(jobId).get();
		}
	};

	private static final Actuator<JobStatus, JobID> GET_JOB_STATUS_ACTUATOR = new Actuator<JobStatus, JobID>() {
		@Override
		public JobStatus execute(RestClusterClient<StandaloneClusterId> client, JobID jobId) throws Exception {
			return client.getJobStatus(jobId).get();
		}
	};

	private static final Actuator<JobResult, JobID> REQUEST_JOB_RESULT_ACTUATOR = new Actuator<JobResult, JobID>() {
		@Override
		public JobResult execute(RestClusterClient<StandaloneClusterId> client, JobID jobId) throws Exception {
			return client.requestJobResult(jobId).get();
		}
	};

	private static final Actuator<String, JobStopParams> STOP_JOB_ACTUATOR = new Actuator<String, JobStopParams>() {
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

	public StandaloneRestClusterClient() {
		super();
	}

	public StandaloneRestClusterClient(Properties properties) {
		super(properties);
	}

	public StandaloneRestClusterClient(String pathInClassPath) {
		super(pathInClassPath);
	}

	@Override
	public JobID submit(FlinkJobs flinkJobs) throws Exception {
		Map<String, String> options = flinkJobs.getOptions();
		String classpaths = config.getProperty("classpaths"),
				parallelism = config.getProperty("parallelism.default", "1");
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

		Properties customConf = load(flinkJobs.getConfiguration());
		Configuration configuration = getConfiguration(customConf);
		Builder builder = PackagedProgram.newBuilder().setConfiguration(configuration)
				.setEntryPointClassName(getEntryPointClassName(flinkJobs)).setJarFile(getJar(flinkJobs))
				.setUserClassPaths(toURLs(classpaths)).setSavepointRestoreSettings(savepointRestoreSettings);

		String arguments = getArguments(flinkJobs);
		if (!isEmptyArguments(arguments)) {
			builder.setArguments(arguments);
		}
		boolean submit = true;
		if (isBlank(flinkJobs.getServiceName())) {
			List<Operate> operates = flinkJobs.getOperates();
			if (operates != null && !operates.isEmpty()) {
				submit = false;
				for (int i = 0, size = operates.size(); i < size; i++) {
					if (!LOCAL_OPERATES.contains(operates.get(i).getType())) {
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
						Integer.parseInt(parallelism), Boolean.valueOf(config.getProperty("suppress.output", "false")));
				return retry(SUBMIT_JOB_ACTUATOR, jobGraph, configuration, customConf);
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
		return retry(CANCEL_JOB_ACTUATOR, jobId, getConfiguration(), null);
	}

	@Override
	public Collection<JobStatusMessage> listJobs() throws Exception {
		return retry(LIST_JOBS_ACTUATOR, null, getConfiguration(), null);
	}

	public JobDetailsInfo getJobDetails(JobID jobId) throws Exception {
		return retry(GET_JOB_DETAILS_ACTUATOR, jobId, getConfiguration(), null);
	}

	@Override
	public JobStatus getJobStatus(JobID jobId) throws Exception {
		return retry(GET_JOB_STATUS_ACTUATOR, jobId, getConfiguration(), null);
	}

	@Override
	public JobResult requestJobResult(JobID jobId) throws Exception {
		return retry(REQUEST_JOB_RESULT_ACTUATOR, jobId, getConfiguration(), null);
	}

	@Override
	public String stop(JobID jobId) throws Exception {
		return retry(STOP_JOB_ACTUATOR, new JobStopParams(jobId, config.getProperty("state.savepoints.dir")),
				getConfiguration(), null);
	}

	@Override
	public RestClusterClient<StandaloneClusterId> getClusterClient() throws Exception {
		return newRestClusterClient(getConfiguration());
	}

	@Override
	public RestClusterClient<StandaloneClusterId> getClusterClient(Properties customConf) throws Exception {
		return newRestClusterClient(getConfiguration(customConf));
	}

	private <R, T> R retry(Actuator<R, T> actuator, T params, Configuration configuration, Properties customConf)
			throws Exception {
		for (int i = 1, size = configurations.size(); i < size; i++) {
			try {
				return tryOnce(actuator, params, configuration);
			} catch (Exception e) {
				log.warn("The " + i + "th attempt failed, trying the " + (i + 1) + "th times");
			}
			configuration = getConfiguration(customConf);
		}
		return tryOnce(actuator, params, configuration);// Try for the last time
	}

	private <R, T> R tryOnce(Actuator<R, T> actuator, T params, Configuration configuration) throws Exception {
		RestClusterClient<StandaloneClusterId> client = null;
		try {
			client = newRestClusterClient(configuration);
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

	private Configuration getConfiguration(Properties customConf) {
		Configuration configuration = getConfiguration();
		if (customConf != null) {
			configuration = configuration.clone();
			configuration.addAll(ConfigurationUtils.createConfiguration(customConf));
		}
		return configuration;
	}

	private RestClusterClient<StandaloneClusterId> newRestClusterClient(Configuration configuration) throws Exception {
		return new RestClusterClient<StandaloneClusterId>(configuration, StandaloneClusterId.getInstance());
	}

	private static List<URL> toURLs(String classpaths) throws MalformedURLException {
		if (isBlank(classpaths)) {
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

	/**
	 * 加载字符串配置
	 * 
	 * @param config 字符串配置
	 * @return 返回配置查找表
	 */
	public static Properties load(String config) {
		if (config == null) {
			return null;
		} else {
			Properties properties = new Properties();
			config = config.trim();
			int len = config.length(), i = 0, backslashes = 0;
			char a = BLANK_SPACE, b = BLANK_SPACE;
			boolean /* 是否在字符串区域 */ isString = false, isKey = true;
			StringBuilder key = new StringBuilder(), value = new StringBuilder();
			while (i < len) {
				char c = config.charAt(i);
				if (isString) {
					if (c == BACKSLASH) {
						backslashes++;
					} else {
						if (isStringEnd(a, b, c, backslashes)) {// 字符串区域结束
							isString = false;
						}
						backslashes = 0;
					}
					if (isKey) {
						key.append(c);
					} else {
						value.append(c);
					}
				} else {
					if (c == SINGLE_QUOTATION_MARK) {// 字符串区域开始
						isString = true;
						if (isKey) {
							key.append(c);
						} else {
							value.append(c);
						}
					} else if (isKey) {
						if (c == VALUE_BEGIN) {
							isKey = false;
						} else {
							key.append(c);
						}
					} else {
						if (VALUE_END.contains(c)) {
							isKey = true;
							put(properties, key, value);
							key.setLength(0);
							value.setLength(0);
							a = b;
							b = c;
							i++;
							for (; i < len; i++) {
								c = config.charAt(i);
								if (c > BLANK_SPACE) {
									break;
								}
								a = b;
								b = c;
							}
							continue;
						} else {
							value.append(c);
						}
					}
				}
				a = b;
				b = c;
				i++;
			}
			if (key.length() > 0) {
				put(properties, key, value);
			}
			return properties;
		}
	}

	/**
	 * 
	 * 根据指定的三个前后相邻字符a、b和c及当前字符c之前的连续反斜杠数量，判断其是否为命名参数脚本字符串区的结束位置
	 * 
	 * @param a           前第二个字符a
	 * @param b           前一个字符b
	 * @param c           当前字符c
	 * @param backslashes 当前字符c之前的连续反斜杠数量
	 * @return 是动态脚本字符串区域结束位置返回true，否则返回false
	 */
	public static boolean isStringEnd(char a, char b, char c, int backslashes) {
		if (c == SINGLE_QUOTATION_MARK) {
			if (b == BACKSLASH) {
				return backslashes % 2 == 0;
			} else {
				return true;
			}
		} else {
			return false;
		}
	}

	private static void put(Properties properties, StringBuilder key, StringBuilder value) {
		String k = key.toString().trim(), v = value.toString().trim();
		int last = k.length() - 1;
		if (k.charAt(0) == SINGLE_QUOTATION_MARK && k.charAt(last) == SINGLE_QUOTATION_MARK) {
			k = k.substring(1, last);
		}
		properties.put(k, v);
	}

}