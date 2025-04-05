package cn.tenmg.clink.config.model;

import java.util.List;

import cn.tenmg.clink.config.model.bsh.Var;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;

/**
 * BeanShell处理配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Bsh extends BasicOperate {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8373030432325030256L;

	@XmlElement(name = "var", namespace = Clink.NAMESPACE)
	private List<Var> vars;

	@XmlElement(namespace = Clink.NAMESPACE)
	private String java;

	/**
	 * 获取变量列表
	 * 
	 * @return 变量列表
	 */
	public List<Var> getVars() {
		return vars;
	}

	/**
	 * 设置变量列表
	 * 
	 * @param vars
	 *            变量列表
	 */
	public void setVars(List<Var> vars) {
		this.vars = vars;
	}

	/**
	 * 获取Java代码
	 * 
	 * @return Java代码
	 */
	public String getJava() {
		return java;
	}

	/**
	 * 设置Java代码
	 * 
	 * @param java
	 *            Java代码
	 */
	public void setJava(String java) {
		this.java = java;
	}

}
