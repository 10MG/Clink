package cn.tenmg.flink.jobs.config.model;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

import cn.tenmg.flink.jobs.config.model.params.Param;

/**
 * 参数集配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Params {

	@XmlElement(namespace = FlinkJobs.NAMESPACE)
	private List<Param> param;

	/**
	 * 获取参数列表
	 * 
	 * @return 参数列表
	 */
	public List<Param> getParam() {
		return param;
	}

	/**
	 * 设置参数列表
	 * 
	 * @param param
	 *            参数列表
	 */
	public void setParam(List<Param> param) {
		this.param = param;
	}

}
