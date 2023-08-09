package cn.tenmg.clink.model;

/**
 * JDBC操作配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.1
 */
public class Jdbc extends Operate {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6395425960958639543L;

	private String dataSource;

	private String method;

	private String script;

	private String resultClass;

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public String getMethod() {
		return method;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public String getScript() {
		return script;
	}

	public void setScript(String script) {
		this.script = script;
	}

	public String getResultClass() {
		return resultClass;
	}

	public void setResultClass(String resultClass) {
		this.resultClass = resultClass;
	}

}
