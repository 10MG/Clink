package cn.tenmg.clink.context;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.tenmg.clink.configuration.ConfigurationLoader;
import cn.tenmg.clink.exception.DataSourceNotFoundException;
import cn.tenmg.clink.exception.IllegalConfigurationException;
import cn.tenmg.clink.utils.ConfigurationUtils;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.PropertiesLoaderUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * Clink上下文
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
@SuppressWarnings({ "unchecked" })
public abstract class ClinkContext {

	private static Logger log = LoggerFactory.getLogger(ClinkContext.class);

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

	public static final String CONFIG_SPLITER = ".", SMART_MODE_CONFIG_KEY = "clink.smart";

	private static final ThreadLocal<Map<Object, Object>> resources = new InheritableThreadLocalMap<Map<Object, Object>>();

	private static final Map<String, Map<String, String>> datasources = MapUtils.newHashMap(),
			datasourceCache = MapUtils.newHashMap();

	private static final Map<String, String> autoDatasource = MapUtils.newHashMap();

	private static final Map<String, String> tableExecConfigs = MapUtils.newHashMap();

	private static final String DEFAULT_STRATEGIES_PATH = "clink-context-loader.properties",
			CONTEXT_LOCATION_KEY = "clink.context", DEFAULT_CONTEXT_LOCATION = "clink-context.properties",
			DATASOURCE_PREFIX = "datasource" + CONFIG_SPLITER,
			AUTO_DATASOURCE_PREFIX = "auto" + CONFIG_SPLITER + DATASOURCE_PREFIX,
			TABLE_API_CONFIG_PREFIX = "table.exec",
			DATA_SYNC_CONFIG_REGEX = "^data\\.sync\\.[^\\.]+\\.((from|to)_type|script|strategy)$",
			EXECUTION_ENVIRONMENT = "ExecutionEnvironment", CURRENT_CONFIGURATION = "CurrentConfiguration",
			IDENTIFIER = "identifier";

	private static Properties config = new Properties();

	static {
		config.putAll(System.getenv());// 系统环境变量
		config.putAll(System.getProperties());// JVM环境变量
		PropertiesLoaderUtils.loadIgnoreException(config, DEFAULT_STRATEGIES_PATH);
		String contextFile = config.getProperty(CONTEXT_LOCATION_KEY, DEFAULT_CONTEXT_LOCATION);
		try {
			PropertiesLoaderUtils.load(config, contextFile);
		} catch (Exception e) {
			log.warn("An exception occurred while loading ".concat(contextFile) + " in classpath", e);
		}

		String loaderClassName = config.getProperty("clink.configuration-loader",
				"cn.tenmg.clink.configuration.loader.PropertiesFileConfigurationLoader");
		ConfigurationLoader loader;
		try {
			Class<?> cls = Class.forName(loaderClassName);
			loader = (ConfigurationLoader) cls.getConstructor().newInstance();
		} catch (ClassNotFoundException e) {
			throw new IllegalConfigurationException("Unable to find configuration loader " + loaderClassName, e);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
				| NoSuchMethodException | SecurityException e) {
			throw new IllegalConfigurationException("Error occurred in instantiation configuration loader", e);
		}
		loader.load(config);

		Object value;
		Entry<Object, Object> entry;
		String key, name, param, keyLowercase;
		Map<String, String> dataSource;
		boolean ignoreCase = !Boolean.valueOf(getProperty("data.sync.case-sensitive", "true"));
		int configSpliterLen = CONFIG_SPLITER.length(), datasourcePrefixLen = DATASOURCE_PREFIX.length(),
				autoDatasourcePrefixLen = AUTO_DATASOURCE_PREFIX.length();
		for (Iterator<Entry<Object, Object>> it = config.entrySet().iterator(); it.hasNext();) {
			entry = it.next();
			key = entry.getKey().toString();
			value = entry.getValue();
			if (key.startsWith(DATASOURCE_PREFIX)) {
				param = key.substring(datasourcePrefixLen);
				int index = param.indexOf(CONFIG_SPLITER);
				if (index > 0) {
					name = param.substring(0, index);
					param = param.substring(index + configSpliterLen);
					dataSource = datasources.get(name);
					if (dataSource == null) {
						dataSource = new LinkedHashMap<String, String>();
						datasources.put(name, dataSource);
					}
					dataSource.put(param, value.toString());
				}
			} else if (key.startsWith(AUTO_DATASOURCE_PREFIX)) {
				param = key.substring(autoDatasourcePrefixLen);
				autoDatasource.put(param, value.toString());
			} else if (key.startsWith(TABLE_API_CONFIG_PREFIX)) {
				tableExecConfigs.put(key, value.toString());
			} else if (ignoreCase && key.matches(DATA_SYNC_CONFIG_REGEX)) {
				keyLowercase = key.toLowerCase();
				if (!key.equals(keyLowercase) && !config.containsKey(keyLowercase)) {
					config.put(keyLowercase, value);
				}
			}
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
			properties.putAll(ClinkContext.getTableExecConfigs());
			loadConfiguration(properties, getCurrentConfiguration());
			tableConfig.addConfiguration(
					org.apache.flink.configuration.ConfigurationUtils.createConfiguration(properties));// 添加配置
			ClinkContext.put(env, tableEnv);
			ClinkContext.put(tableEnv, tableEnv.getCurrentCatalog());
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
			ClinkContext.put(tableEnv, catalog);
		}
		return catalog;
	}

	/**
	 * 根据键获取配置的属性
	 * 
	 * @param key
	 *            键
	 * @return 配置属性值或null
	 */
	public static String getProperty(String key) {
		return config.getProperty(key);
	}

	/**
	 * 根据优先键依次获取配置的属性，一旦某一键存在则返回其对应的值，均不存在则返回 {@code null}
	 * 
	 * @param priorKeys
	 *            优先键
	 * @return 配置属性值或 {@code null}
	 */
	public static String getProperty(List<String> priorKeys) {
		return getProperty(priorKeys, null);
	}

	/**
	 * 根据键获取配置的属性，不存在则返回默认值
	 * 
	 * @param key
	 *            键
	 * @param defaultValue
	 *            默认值
	 * @return 配置属性值或默认值
	 */
	public static String getProperty(String key, String defaultValue) {
		return config.getProperty(key, defaultValue);
	}

	/**
	 * 根据优先键依次获取配置的属性，一旦某一键存在则返回其对应的值，均不存在则返回默认值
	 * 
	 * @param priorKeys
	 *            优先键
	 * @param defaultValue
	 *            默认值
	 * @return 配置属性值或默认值
	 */
	public static String getProperty(List<String> priorKeys, String defaultValue) {
		return ConfigurationUtils.getProperty(config, priorKeys, defaultValue);
	}

	/**
	 * 根据数据库产品名称（小写）获取默认JDBC驱动类名
	 * 
	 * @param productName
	 *            数据库产品名称（小写）
	 * @return 默认JDBC驱动类名
	 */
	public static String getDefaultJDBCDriver(String productName) {
		return getProperty(StringUtils.concat("jdbc", CONFIG_SPLITER, productName, CONFIG_SPLITER, "driver"));
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
		return datasources;
	}

	/**
	 * 根据数据源名称获取数据源。如果指定数据源不存在将抛出cn.tenmg.clink.exception.DataSourceNotFoundException
	 * 
	 * @param name
	 *            数据源名称
	 * @return 数据源
	 */
	public static Map<String, String> getDatasource(String name) {
		Map<String, String> dataSource = datasources.get(name);
		if (dataSource == null) {
			if (autoDatasource.isEmpty()) {
				throw new DataSourceNotFoundException(
						"DataSource named " + name + " not found, Please check the configuration");
			} else {
				dataSource = datasourceCache.get(name);
				if (dataSource == null) {
					synchronized (datasourceCache) {
						dataSource = datasourceCache.get(name);
						if (dataSource == null) {
							log.debug("Generated and cached the DataSource named " + name + " automatically");
							dataSource = MapUtils.toHashMapBuilder(autoDatasource).build(autoDatasource.get(IDENTIFIER),
									name);
							dataSource.remove(IDENTIFIER);
							datasourceCache.put(name, dataSource);
						}
					}
				} else {
					log.debug("Get automatically generated DataSource named " + name + " from cache");
				}
			}
		}
		return dataSource;
	}

	/**
	 * 获取 {@code Flink Table & SQL} 的运行配置
	 * 
	 * @return 返回 {@code Flink Table&SQL} 的运行配置
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

	public Properties getConfig(String keyPrefix) {
		return null;
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
