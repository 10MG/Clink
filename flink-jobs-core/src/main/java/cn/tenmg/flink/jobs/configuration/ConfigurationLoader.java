package cn.tenmg.flink.jobs.configuration;

import java.util.Properties;

import cn.tenmg.flink.jobs.exception.ConfigurationLoadException;

/**
 * 配置加载器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public interface ConfigurationLoader {

	/**
	 * 加载配置
	 * 
	 * @param config
	 *            配置对象
	 * @throws ConfigurationLoadException
	 *             配置加载异常
	 */
	void load(Properties config) throws ConfigurationLoadException;

}
