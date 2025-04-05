package cn.tenmg.clink.model.create.table;

import java.io.Serializable;

/**
 * 数据列
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.3.0
 */
public class Column implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7744054869336052412L;

	private String name;

	private String type;

	/**
	 * 获取列名
	 * 
	 * @return 返回列名
	 */
	public String getName() {
		return name;
	}

	/**
	 * 设置列名
	 * 
	 * @param name
	 *            列名
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * 获取列的数据类型
	 * 
	 * @return 返回列的数据类型
	 */
	public String getType() {
		return type;
	}

	/**
	 * 设置列的数据类型
	 * 
	 * @param type
	 *            列的数据类型
	 */
	public void setType(String type) {
		this.type = type;
	}

}
