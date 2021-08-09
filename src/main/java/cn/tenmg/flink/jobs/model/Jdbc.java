package cn.tenmg.flink.jobs.model;

/**
 * JDBC操作配置模型
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 * 
 * @since 1.1.1
 */
public class Jdbc extends Operate {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6395425960958639543L;

	private String dataSource;

	private String method = "executeLargeUpdate";

	private String script;

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

}
