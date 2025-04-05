package cn.tenmg.clink.config.model;

import java.util.List;

import cn.tenmg.clink.config.model.params.Param;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;

/**
 * 参数集配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Params {

	@XmlElement(namespace = Clink.NAMESPACE)
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
