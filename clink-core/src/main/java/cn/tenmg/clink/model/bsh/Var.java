package cn.tenmg.clink.model.bsh;

import java.io.Serializable;

/**
 * 变量
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.1.0
 */
public class Var implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7677686936463221422L;

	private String name;

	private String value;

	/**
	 * 获取变量名
	 * 
	 * @return 变量名
	 */
	public String getName() {
		return name;
	}

	/**
	 * 设置变量名
	 * 
	 * @param name
	 *            变量名
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * 获取变量值
	 * 
	 * @return 变量值
	 */
	public String getValue() {
		return value;
	}

	/**
	 * 设置变量值
	 * 
	 * @param value
	 *            变量值
	 */
	public void setValue(String value) {
		this.value = value;
	}

}
