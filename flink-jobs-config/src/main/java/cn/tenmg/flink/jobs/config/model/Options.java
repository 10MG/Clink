package cn.tenmg.flink.jobs.config.model;

import java.util.Map;

/**
 * 运行选项配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
public class Options {

	private String keyPrefix = "--";

	private Map<String, String> option;

	/**
	 * 获取运行选项默认前缀
	 * 
	 * @return 运行选项默认前缀
	 */
	public String getKeyPrefix() {
		return keyPrefix;
	}

	/**
	 * 设置运行选项的默认前缀。如不设置默认为“--”
	 * 
	 * @param keyPrefix
	 *            运行选项默认前缀
	 */
	public void setKeyPrefix(String keyPrefix) {
		this.keyPrefix = keyPrefix;
	}

	/**
	 * 获取运行选项查找表
	 * 
	 * @return 返回运行选项查找表
	 */
	public Map<String, String> getOption() {
		return option;
	}

	/**
	 * 设置运行选项查找表
	 * 
	 * @param option
	 *            运行选项查找表
	 */
	public void setOption(Map<String, String> option) {
		this.option = option;
	}

}
