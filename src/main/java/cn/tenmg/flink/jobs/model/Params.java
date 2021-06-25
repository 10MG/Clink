package cn.tenmg.flink.jobs.model;

import java.io.Serializable;
import java.util.Date;

import org.apache.flink.api.common.RuntimeExecutionMode;

/**
 * flink-jobs应用入口参数
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 */
public class Params implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8430798989924072785L;

	/**
	 * 服务名称
	 */
	private String serviceName;

	/**
	 * 运行模式
	 */
	private RuntimeExecutionMode runtimeMode;

	/**
	 * 开始时间
	 */
	private Date beginTime;

	/**
	 * 截止时间
	 */
	private Date endTime;

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public RuntimeExecutionMode getRuntimeMode() {
		return runtimeMode;
	}

	public void setRuntimeMode(RuntimeExecutionMode runtimeMode) {
		this.runtimeMode = runtimeMode;
	}

	public Date getBeginTime() {
		return beginTime;
	}

	public void setBeginTime(Date beginTime) {
		this.beginTime = beginTime;
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}

}