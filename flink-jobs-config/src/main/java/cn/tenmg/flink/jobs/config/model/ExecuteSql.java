package cn.tenmg.flink.jobs.config.model;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;

/**
 * Flink SQL的sqlQuery操作配置
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.4
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class ExecuteSql extends SqlQuery {

	/**
	 * 
	 */
	private static final long serialVersionUID = -442826507697198239L;

	@XmlAttribute
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
