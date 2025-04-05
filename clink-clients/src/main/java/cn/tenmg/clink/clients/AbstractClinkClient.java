package cn.tenmg.clink.clients;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.filter.PropertyFilter;

import cn.tenmg.clink.ClinkClient;
import cn.tenmg.clink.clients.configuration.ConfigurationLoader;
import cn.tenmg.clink.clients.exception.ConfigurationLoadException;
import cn.tenmg.clink.config.model.Clink;
import cn.tenmg.dsl.utils.PropertiesLoaderUtils;
import cn.tenmg.dsl.utils.SetUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * Clink客户端抽象类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.0
 *
 * @param <T>
 *            flink集群客户端的类型
 */
public abstract class AbstractClinkClient<T extends ClusterClient<?>> implements ClinkClient<T> {

	protected static final String DEFAULT_STRATEGIES_PATH = "clink-context-loader.properties",
			DEFAULT_CONFIG_LOCATION = "clink.properties", FLINK_JOBS_DEFAULT_JAR_KEY = "clink.default.jar",
			FLINK_JOBS_DEFAULT_CLASS_KEY = "clink.default.class", NACOS_CONFIG_PREFIX = "nacos.config.",
			EMPTY_ARGUMENTS = "{}";
	protected static final Set<String> EXCLUDES = SetUtils.newHashSet("options", "mainClass", "jar", "allwaysNewJob");

	protected Logger log = LoggerFactory.getLogger(getClass());

	protected Properties config = new Properties();
	{
		config.putAll(System.getenv());// 系统环境变量
		config.putAll(System.getProperties());// JVM环境变量
		PropertiesLoaderUtils.loadIgnoreException(config, DEFAULT_STRATEGIES_PATH);
	}

	protected final Queue<Configuration> configurations = new LinkedList<Configuration>();

	public AbstractClinkClient() {
		init(config.getProperty("clink.configuration-file", DEFAULT_CONFIG_LOCATION));
	}

	public AbstractClinkClient(String pathInClassPath) {
		init(pathInClassPath);
	}

	public AbstractClinkClient(Properties properties) {
		init(properties);
	}

	protected void init(String pathInClassPath) {
		init(PropertiesLoaderUtils.loadIgnoreException(pathInClassPath));
	}

	protected void init(Properties properties) {
		config.putAll(properties);
		String className = config.getProperty("clink.clients.configuration-loader");
		if (StringUtils.isNotBlank(className)) {
			try {
				ConfigurationLoader loader = (ConfigurationLoader) Class.forName(className).getConstructor()
						.newInstance();
				loader.load(config);
			} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
					| InvocationTargetException | NoSuchMethodException | SecurityException
					| ClassNotFoundException e) {
				throw new ConfigurationLoadException("Unable to load configuration", e);
			}
		}
		Configuration configuration = ConfigurationUtils.createConfiguration(config);
		String rpcServers = config.getProperty("jobmanager.rpc.servers");
		String address = config.getProperty("rest.addresses", config.getProperty("rest.address"));
		if (!isBlank(address)) {// 新的方式
			Configuration config;
			String addresses[] = address.split(","), addr[];
			for (int i = 0; i < addresses.length; i++) {
				config = configuration.clone();
				addr = addresses[i].split(":", 2);
				config.set(RestOptions.ADDRESS, addr[0].trim());
				if (addr.length > 1) {
					config.set(RestOptions.PORT, Integer.parseInt(addr[1].trim()));
				} else if (!config.contains(RestOptions.PORT)) {
					config.set(RestOptions.PORT, 8081);
				}
				this.configurations.add(config);
			}
		} else if (isBlank(rpcServers)) {
			this.configurations.add(configuration);
		} else {// 兼容fallback
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
	 * 获取运行的JAR。如果Clink配置对象没有配置运行的JAR则返回配置文件中配置的默认JAR，如果均没有，则返回<code>null</code>
	 * 
	 * @param clink
	 *            Clink配置对象
	 * @return 返回运行的JAR
	 */
	protected File getJar(Clink clink) {
		String jar = getJarPath(clink);
		if (isBlank(jar)) {
			return null;
		}
		return new File(jar);
	}

	/**
	 * 获取入口类名
	 * 
	 * @param clink
	 *            Clink配置对象
	 * @return 返回入口类名
	 */
	protected String getEntryPointClassName(Clink clink) {
		String mainClass = clink.getMainClass();
		if (isBlank(mainClass) && isBlank(getJarPath(clink))) {
			mainClass = config.getProperty(FLINK_JOBS_DEFAULT_CLASS_KEY, "cn.tenmg.clink.ClinkPortal");
		}
		return mainClass;
	}

	/**
	 * 获取flink程序运行参数
	 * 
	 * @param clink
	 *            Clink配置对象
	 * @return 返回运行
	 */
	protected static String getArguments(Clink clink) {
		return JSON.toJSONString(clink, new PropertyFilter() {
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
		return isBlank(arguments) || "{}".equals(arguments);
	}

	/**
	 * 获取运行的JAR文件位置
	 * 
	 * @param clink
	 *            Clink配置对象
	 * @return 运行的JAR文件位置
	 */
	protected String getJarPath(Clink clink) {
		String jar = clink.getJar();
		if (isBlank(jar)) {
			jar = config.getProperty(FLINK_JOBS_DEFAULT_JAR_KEY);
		}
		return jar;
	}

	/**
	 * 判断指定字符串是否为空（<code>null</code>）、空字符串（<code>""</code>）或者仅含空格的字符串
	 * 
	 * @param string
	 *            指定字符串
	 * @return 指定字符串为空（<code>null</code>）、空字符串（<code>""</code>）或者仅含空格的字符串返回
	 *         <code>true</code>，否则返回<code>false</code>
	 */
	protected static boolean isBlank(String string) {
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
