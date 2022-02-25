package cn.tenmg.flink.jobs.model;

/**
 * Flink SQL的sqlQuery操作配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.0
 *
 */
public class ExecuteSql extends SqlQuery {

	/**
	 * 
	 */
	private static final long serialVersionUID = -442826507697198239L;

	private String dataSource;

	/**
	 * 获取使用的数据源名称
	 * 
	 * @return 使用的数据源名称
	 */
	public String getDataSource() {
		return dataSource;
	}

	/**
	 * 设置使用的数据源名称
	 * 
	 * @param dataSource
	 *            使用的数据源名称
	 */
	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

}
