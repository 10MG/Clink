package cn.tenmg.flink.jobs.config.model;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * 运行选项配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Options {

	@XmlElement(namespace = FlinkJobs.NAMESPACE)
	private List<Option> option;

	/**
	 * 获取运行选项列表
	 * 
	 * @return 返回运行选项列表
	 */
	public List<Option> getOption() {
		return option;
	}

	/**
	 * 设置运行选项列表
	 * 
	 * @param option
	 *            运行选项列表
	 */
	public void setOption(List<Option> option) {
		this.option = option;
	}

}
