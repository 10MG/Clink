package cn.tenmg.clink.model;

/**
 * Flink SQL的sqlQuery操作配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 */
public class SqlQuery extends Operate {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6902349255064231962L;

	private String catalog;

	private String script;

	/**
	 * 获取使用的目录
	 * 
	 * @return 使用的目录
	 */
	public String getCatalog() {
		return catalog;
	}

	/**
	 * 设置使用的目录
	 * 
	 * @param catalog
	 *            使用的目录
	 */
	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}

	/**
	 * 获取SQL脚本
	 * 
	 * @return SQL脚本
	 */
	public String getScript() {
		return script;
	}

	/**
	 * 设置SQL脚本
	 * 
	 * @param script
	 *            SQL脚本
	 */
	public void setScript(String script) {
		this.script = script;
	}
}
