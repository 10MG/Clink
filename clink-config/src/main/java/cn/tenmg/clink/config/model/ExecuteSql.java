package cn.tenmg.clink.config.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;

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

	@XmlAttribute
	private String dataSourceFilter;

	/**
	 * 获取数据源名称
	 * 
	 * @return 数据源名称
	 */
	public String getDataSource() {
		return dataSource;
	}

	/**
	 * 设置数据源名称
	 * 
	 * @param dataSource
	 *            数据源名称
	 */
	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	/**
	 * 获取数据源过滤器
	 * 
	 * @return 数据源过滤器
	 */
	public String getDataSourceFilter() {
		return dataSourceFilter;
	}

	/**
	 * 设置数据源过滤器
	 * 
	 * @param dataSourceFilter
	 *            数据源过滤器
	 */
	public void setDataSourceFilter(String dataSourceFilter) {
		this.dataSourceFilter = dataSourceFilter;
	}

}
