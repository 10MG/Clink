package cn.tenmg.flink.jobs.clients;

import java.io.File;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.PropertyFilter;

import cn.tenmg.flink.jobs.FlinkJobsClient;
import cn.tenmg.flink.jobs.clients.utils.PropertiesLoaderUtils;
import cn.tenmg.flink.jobs.clients.utils.Sets;
import cn.tenmg.flink.jobs.config.model.FlinkJobs;

/**
 * flink-jobs客户端抽象类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.0
 *
 * @param <T>
 *            flink集群客户端的类型
 */
public abstract class AbstractFlinkJobsClient<T extends ClusterClient<?>> implements FlinkJobsClient<T> {

	protected static final String FLINK_JOBS_DEFAULT_JAR_KEY = "flink.jobs.default.jar",
			FLINK_JOBS_DEFAULT_CLASS_KEY = "flink.jobs.default.class";

	protected static final Set<String> EXCLUDES = Sets.as("options", "mainClass", "jar", "allwaysNewJob");

	protected static final String EMPTY_ARGUMENTS = "{}";

	protected Logger log = LoggerFactory.getLogger(getClass());

	protected Properties properties;

	protected final Queue<Configuration> configurations = new LinkedList<Configuration>();

	public AbstractFlinkJobsClient() {
		super();
		init("flink-jobs-clients.properties");
	}

	public AbstractFlinkJobsClient(String pathInClassPath) {
		super();
		init(pathInClassPath);
	}

	public AbstractFlinkJobsClient(Properties properties) {
		super();
		init(properties);
	}

	protected void init(String pathInClassPath) {
		try {
			properties = PropertiesLoaderUtils.loadFromClassPath(pathInClassPath);
		} catch (Exception e) {
			properties = new Properties();
			log.error("Failed to load configuration file " + pathInClassPath);
		}
		init(properties);
	}

	protected void init(Properties properties) {
		this.properties = properties;
		Configuration configuration = ConfigurationUtils.createConfiguration(properties);
		String rpcServers = properties.getProperty("jobmanager.rpc.servers");
		if (isBlank(rpcServers)) {
			this.configurations.add(configuration);
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
				this.configurations.add(config);
			}
		}
	}

	/**
	 * 获取运行的JAR。如果flink-jobs配置对象没有配置运行的JAR则返回配置文件中配置的默认JAR，如果均没有，则返回<code>null</code>
	 * 
	 * @param flinkJobs
	 *            flink-jobs配置对象
	 * @return 返回运行的JAR
	 */
	protected File getJar(FlinkJobs flinkJobs) {
		String jar = flinkJobs.getJar();
		if (jar == null) {
			jar = properties.getProperty(FLINK_JOBS_DEFAULT_JAR_KEY);
		}
		if (jar == null) {
			return null;
		}
		return new File(jar);
	}

	/**
	 * 获取入口类名
	 * 
	 * @param flinkJobs
	 *            flink-jobs配置对象
	 * @return 返回入口类名
	 */
	protected String getEntryPointClassName(FlinkJobs flinkJobs) {
		String mainClass = flinkJobs.getMainClass();
		if (mainClass == null) {
			mainClass = properties.getProperty(FLINK_JOBS_DEFAULT_CLASS_KEY);
		}
		return mainClass;
	}

	/**
	 * 获取flink程序运行参数
	 * 
	 * @param flinkJobs
	 *            flink-jobs配置对象
	 * @return 返回运行
	 */
	protected static String getArguments(FlinkJobs flinkJobs) {
		return JSON.toJSONString(flinkJobs, new PropertyFilter() {
			@Override
			public boolean apply(Object object, String name, Object value) {
				if (EXCLUDES.contains(name)) {// 排除在外的字段
					return false;
				}
				return true;
			}
		});
	}

	/**
	 * 判断运行参数是否为空
	 * 
	 * @param arguments
	 *            运行参数
	 * @return true/false
	 */
	protected static Boolean isEmptyArguments(String arguments) {
		return arguments == null || "{}".equals(arguments) || "".equals(arguments.trim());
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
