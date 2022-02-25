package cn.tenmg.flink.jobs.model;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.RuntimeExecutionMode;

/**
 * flink-jobs应用入口参数
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.1.0
 */
public class Arguments implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8430798989924072785L;

	private String serviceName;

	private RuntimeExecutionMode runtimeMode;

	private String configuration;

	private Map<String, Object> params;

	private List<String> operates;

	/**
	 * 获取服务名称
	 * 
	 * @return 返回服务名称
	 */
	public String getServiceName() {
		return serviceName;
	}

	/**
	 * 设置服务名称
	 * 
	 * @param serviceName
	 *            服务名称
	 */
	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	/**
	 * 获取运行模式
	 * 
	 * @return 返回运行模式
	 */
	public RuntimeExecutionMode getRuntimeMode() {
		return runtimeMode;
	}

	/**
	 * 设置运行模式
	 * 
	 * @param runtimeMode
	 *            运行模式
	 */
	public void setRuntimeMode(RuntimeExecutionMode runtimeMode) {
		this.runtimeMode = runtimeMode;
	}

	/**
	 * 获取配置信息
	 * 
	 * @return 配置信息
	 */
	public String getConfiguration() {
		return configuration;
	}

	/**
	 * 设置配置信息
	 * 
	 * @param configuration
	 *            配置信息
	 */
	public void setConfiguration(String configuration) {
		this.configuration = configuration;
	}

	/**
	 * 获取参数查找表
	 * 
	 * @return 参数查找表
	 */
	public Map<String, Object> getParams() {
		return params;
	}

	/**
	 * 设置参数查找表
	 * 
	 * @param params
	 *            参数查找表
	 */
	public void setParams(Map<String, Object> params) {
		this.params = params;
	}

	/**
	 * 获取执行的操作列表（每个操作使用JSON字符串表示）
	 * 
	 * @return 返回执行的操作列表
	 */
	public List<String> getOperates() {
		return operates;
	}

	/**
	 * 设置执行的操作列表（每个操作使用JSON字符串表示）
	 * 
	 * @param operates
	 *            执行的操作列表
	 */
	public void setOperates(List<String> operates) {
		this.operates = operates;
	}

}