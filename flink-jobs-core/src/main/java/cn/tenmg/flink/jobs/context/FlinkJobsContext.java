package cn.tenmg.flink.jobs.context;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.tenmg.dsl.utils.PropertiesLoaderUtils;
import cn.tenmg.flink.jobs.exception.DataSourceNotFoundException;
import cn.tenmg.flink.jobs.utils.ConfigurationUtils;
import cn.tenmg.flink.jobs.utils.PlaceHolderUtils;

/**
 * flink-jobs上下文
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
@SuppressWarnings({ "unchecked" })
public abstract class FlinkJobsContext {

	private static Logger log = LoggerFactory.getLogger(FlinkJobsContext.class);

	private static final class InheritableThreadLocalMap<T extends Map<Object, Object>>
			extends InheritableThreadLocal<Map<Object, Object>> {

		/**
		 * This implementation was added to address a <a href=
		 * "http://jsecurity.markmail.org/search/?q=#query:+page:1+mid:xqi2yxurwmrpqrvj+state:results"
		 * > user-reported issue</a>.
		 * 
		 * @param parentValue
		 *            the parent value, a HashMap as defined in the
		 *            {@link #initialValue()} method.
		 * @return the HashMap to be used by any parent-spawned child threads (a clone
		 *         of the parent HashMap).
		 */
		protected Map<Object, Object> childValue(Map<Object, Object> parentValue) {
			if (parentValue != null) {
				return (Map<Object, Object>) ((HashMap<Object, Object>) parentValue).clone();
			} else {
				return null;
			}
		}
	}

	public static final String CONFIG_SPLITER = ".", SMART_MODE_CONFIG_KEY = "flink.jobs.smart";

	private static final ThreadLocal<Map<Object, Object>> resources = new InheritableThreadLocalMap<Map<Object, Object>>();

	private static final Map<String, Map<String, String>> dataSources = new HashMap<String, Map<String, String>>();

	private static final Map<String, String> tableExecConfigs = new HashMap<String, String>();

	private static final String DEFAULT_STRATEGIES_PATH = "flink-jobs-context-loader.properties",
			CONFIG_LOCATION_KEY = "config.location", CONTEXT_LOCATION_KEY = "context.location",
			DATASOURCE_PREFIX = "datasource" + CONFIG_SPLITER,
			DATASOURCE_REGEX = "^" + DATASOURCE_PREFIX.replaceAll("\\.", "\\\\.") + "([\\S]+\\.){0,1}[^\\.]+$",
			EXECUTION_ENVIRONMENT = "ExecutionEnvironment", CURRENT_CONFIGURATION = "CurrentConfiguration";

	private static final int CONFIG_SPLITER_LEN = CONFIG_SPLITER.length(),
			DATASOURCE_PREFIX_LEN = DATASOURCE_PREFIX.length();

	private static Properties defaultProperties, configProperties;

	static {
		try {
			defaultProperties = PropertiesLoaderUtils.loadFromClassPath(DEFAULT_STRATEGIES_PATH);
		} catch (Exception e) {
			log.warn(DEFAULT_STRATEGIES_PATH + " not found in the classpath.", e);
			defaultProperties = new Properties();
		}
		String contextFile = defaultProperties.getProperty(CONTEXT_LOCATION_KEY, "flink-jobs-context.properties");
		try {
			defaultProperties.putAll(PropertiesLoaderUtils.loadFromClassPath(contextFile));
		} catch (Exception e) {
			log.warn(contextFile + " not found in the classpath.", e);
		}
		String configurationFile = getConfigurationFile();
		try {
			configProperties = PropertiesLoaderUtils.loadFromClassPath(configurationFile);
			Entry<Object, Object> entry;
			Object value;
			for (Iterator<Entry<Object, Object>> it = configProperties.entrySet().iterator(); it.hasNext();) {
				entry = it.next();
				value = entry.getValue();
				if (value != null) {
					configProperties.put(entry.getKey(), PlaceHolderUtils.replace(value.toString(), configProperties));
				}
			}
			String key, name, param, keyLowercase;
			Map<String, String> dataSource;
			boolean ignoreCase = !Boolean.valueOf(getProperty("data.sync.timestamp.case_sensitive"));
			for (Iterator<Entry<Object, Object>> it = configProperties.entrySet().iterator(); it.hasNext();) {
				entry = it.next();
				key = entry.getKey().toString();
				value = entry.getValue();
				if (key.matches(DATASOURCE_REGEX)) {
					param = key.substring(DATASOURCE_PREFIX_LEN);
					int index = param.indexOf(CONFIG_SPLITER);
					if (index > 0) {
						name = param.substring(0, index);
						param = param.substring(index + CONFIG_SPLITER_LEN);
						dataSource = dataSources.get(name);
						if (dataSource == null) {
							dataSource = new LinkedHashMap<String, String>();
							dataSources.put(name, dataSource);
						}
						dataSource.put(param, value.toString());
					}
				} else if (key.startsWith("table.exec")) {
					tableExecConfigs.put(key, value.toString());
				} else if (ignoreCase && key.matches("^data\\.sync\\.[^\\.]+\\.((from|to)_type|script|strategy)$")) {
					keyLowercase = key.toLowerCase();
					if (!key.equals(keyLowercase) && !defaultProperties.containsKey(keyLowercase)) {
						defaultProperties.put(keyLowercase, value);
					}
				}
			}
		} catch (Exception e) {
			log.info("Configuration file " + configurationFile
					+ " not found in classpath, the default configuration will be used.");
			configProperties = new Properties();
		}
	}

	/**
	 * 获取当前作业的配置信息
	 * 
	 * @return 当前作业的配置信息
	 */
	public static String getCurrentConfiguration() {
		return (String) get(CURRENT_CONFIGURATION);
	}

	/**
	 * 获取流运行环境
	 * 
	 * @return 流运行环境
	 */
	public static StreamExecutionEnvironment getExecutionEnvironment() {
		StreamExecutionEnvironment env = (StreamExecutionEnvironment) get(EXECUTION_ENVIRONMENT);
		if (env == null) {
			env = StreamExecutionEnvironment.getExecutionEnvironment();
			put(EXECUTION_ENVIRONMENT, env);
		}
		return env;
	}

	/**
	 * 
	 * 使用特定配置信息获取流运行环境
	 * 
	 * @param configuration
	 *            配置信息
	 * @return 流运行环境
	 */
	public static StreamExecutionEnvironment getExecutionEnvironment(String configuration) {
		if (configuration == null) {
			return getExecutionEnvironment();
		}
		StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment(org.apache.flink.configuration.ConfigurationUtils
						.createConfiguration(loadConfiguration(new Properties(), configuration)));
		put(CURRENT_CONFIGURATION, configuration);
		put(EXECUTION_ENVIRONMENT, env);
		return env;
	}

	/**
	 * 
	 * 获取或创建流表环境
	 * 
	 * @return 流表环境
	 */
	public static StreamTableEnvironment getOrCreateStreamTableEnvironment() {
		return getOrCreateStreamTableEnvironment(getExecutionEnvironment());
	}

	/**
	 * 
	 * 获取或创建流表环境
	 * 
	 * @param env
	 *            流运行环境
	 * @return 流表环境
	 */
	public static StreamTableEnvironment getOrCreateStreamTableEnvironment(StreamExecutionEnvironment env) {
		StreamTableEnvironment tableEnv = (StreamTableEnvironment) get(env);
		if (tableEnv == null) {
			tableEnv = StreamTableEnvironment.create(env);
			TableConfig tableConfig = tableEnv.getConfig();
			Properties properties = new Properties();
			properties.putAll(FlinkJobsContext.getTableExecConfigs());
			loadConfiguration(properties, getCurrentConfiguration());
			tableConfig.addConfiguration(
					org.apache.flink.configuration.ConfigurationUtils.createConfiguration(properties));// 添加配置
			FlinkJobsContext.put(env, tableEnv);
			FlinkJobsContext.put(tableEnv, tableEnv.getCurrentCatalog());
		}
		return tableEnv;
	}

	/**
	 * 获取默认目录。先从当前上下文中获取已缓存的默认目录，结果为null则从流表环境获取当前目录并缓存到当前上下文中
	 * 
	 * @param tableEnv
	 *            流表环境
	 * @return 默认目录
	 */
	public static String getDefaultCatalog(StreamTableEnvironment tableEnv) {
		String catalog = (String) get(tableEnv);
		if (catalog == null) {
			catalog = tableEnv.getCurrentCatalog();
			FlinkJobsContext.put(tableEnv, catalog);
		}
		return catalog;
	}

	/**
	 * 根据键获取配置的属性。优先查找用户配置属性，如果用户配置属性不存在从上下文配置中查找
	 * 
	 * @param key
	 *            键
	 * @return 配置属性值或null
	 */
	public static String getProperty(String key) {
		return configProperties.containsKey(key) ? configProperties.getProperty(key)
				: defaultProperties.getProperty(key);
	}

	/**
	 * 根据数据库产品名称（小写）获取默认JDBC驱动类名
	 * 
	 * @param productName
	 *            数据库产品名称（小写）
	 * @return 默认JDBC驱动类名
	 */
	public static String getDefaultJDBCDriver(String productName) {
		return getProperty("jdbc" + CONFIG_SPLITER + productName + CONFIG_SPLITER + "driver");
	}

	/**
	 * 获取当前线程上下文资源
	 * 
	 * @return 返回当前线程上下文资源(一个Map)
	 */
	public static Map<Object, Object> getResources() {
		if (resources.get() == null) {
			return Collections.emptyMap();
		} else {
			return new HashMap<Object, Object>(resources.get());
		}
	}

	/**
	 * 将指定资源放入当前线程上下文
	 * 
	 * @param newResources
	 *            指定资源
	 */
	public static void setResources(Map<Object, Object> newResources) {
		if (newResources == null || newResources.isEmpty()) {
			return;
		}
		ensureResourcesInitialized();
		resources.get().clear();
		resources.get().putAll(newResources);
	}

	/**
	 * 获取数据源查找表
	 * 
	 * @return 数据源查找表
	 */
	public static Map<String, Map<String, String>> getDatasources() {
		return dataSources;
	}

	/**
	 * 获取实际使用的配置文件
	 * 
	 * @return 返回实际使用的配置文件
	 */
	public static String getConfigurationFile() {
		return defaultProperties.getProperty(CONFIG_LOCATION_KEY, "flink-jobs.properties");
	}

	/**
	 * 根据数据源名称获取数据源。如果指定数据源不存在将抛出cn.tenmg.flink.jobs.exception.DataSourceNotFoundException
	 * 
	 * @param name
	 *            数据源名称
	 * @return 数据源
	 */
	public static Map<String, String> getDatasource(String name) {
		Map<String, String> dataSource = dataSources.get(name);
		if (dataSource == null) {
			throw new DataSourceNotFoundException("DataSource named " + name
					+ " not found, Please check the configuration file " + getConfigurationFile());
		}
		return dataSource;
	}

	/**
	 * 获取Table API & SQL的运行配置
	 * 
	 * @return 返回Table API & SQL的运行配置
	 */
	public static Map<String, String> getTableExecConfigs() {
		return tableExecConfigs;
	}

	/**
	 * 根据指定唯一标识获取当前线程上下文资源
	 * 
	 * @param key
	 *            指定唯一标识
	 * @return 返回指定唯一标识所对应的当前线程上下文资源
	 */
	public static Object get(Object key) {
		return getValue(key);
	}

	/**
	 * 用指定唯一标识设置指定对象为当前线程上下文资源
	 * 
	 * @param key
	 *            指定唯一标识
	 * @param value
	 *            指定对象
	 */
	public static void put(Object key, Object value) {
		if (key == null) {
			throw new IllegalArgumentException("key cannot be null");
		}
		if (value == null) {
			remove(key);
			return;
		}
		ensureResourcesInitialized();
		resources.get().put(key, value);
	}

	/**
	 * 使用指定的唯一标识移除当前线程上下文资源
	 * 
	 * @param key
	 *            指定的唯一标识
	 * @return 返回被移除的当前线程上下文资源
	 */
	public static Object remove(Object key) {
		Map<Object, Object> perThreadResources = resources.get();
		return perThreadResources != null ? perThreadResources.remove(key) : null;
	}

	/**
	 * 移除当前线程的上下文资源
	 */
	public static void remove() {
		resources.remove();
	}

	/**
	 * 根据指定唯一标识获取当前线程上下文资源
	 * 
	 * @param key
	 *            指定唯一标识
	 * @return 返回指定唯一标识所对应的当前线程上下文资源
	 */
	private static Object getValue(Object key) {
		Map<Object, Object> perThreadData = resources.get();
		return perThreadData != null ? perThreadData.get(key) : null;
	}

	/**
	 * 确保资源存储空间已初始化
	 */
	private static void ensureResourcesInitialized() {
		if (resources.get() == null) {
			resources.set(new HashMap<Object, Object>());
		}
	}

	// 加载配置
	private static Properties loadConfiguration(Properties properties, String configuration) {
		Map<String, String> config = ConfigurationUtils.load(configuration);
		if (config != null) {
			properties.putAll(config);
		}
		return properties;
	}

}
