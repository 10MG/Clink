package cn.tenmg.clink.config.model;

import java.io.Serializable;
import java.util.List;

import cn.tenmg.clink.config.model.create.table.Column;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;

/**
 * Flink SQL的建表操作配置
 *
 * @author dufeng
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.3.0
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class CreateTable implements Operate, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1481225939687739536L;

	@XmlAttribute
	private String saveAs;

	@XmlAttribute
	private String when;

	@XmlAttribute
	private String catalog;

	/**
	 * 数据源名称
	 */
	@XmlAttribute
	private String dataSource;

	@XmlAttribute
	private String dataSourceFilter;

	/**
	 * 创建的表名
	 */
	@XmlAttribute
	private String tableName;

	/**
	 * 绑定的表名，即WITH子句的“table-name”
	 */
	@XmlAttribute
	private String bindTableName;

	/**
	 * 主键，多个列名以“,”分隔。当开启智能模式时，会自动获取主键信息。
	 */
	@XmlAttribute
	private String primaryKey;

	@XmlAttribute
	private Boolean smart;

	@XmlElement(name = "column", namespace = Clink.NAMESPACE)
	private List<Column> columns;

	@Override
	public String getType() {
		return getClass().getSimpleName();
	}

	@Override
	public String getSaveAs() {
		return saveAs;
	}

	public void setSaveAs(String saveAs) {
		this.saveAs = saveAs;
	}

	@Override
	public String getWhen() {
		return when;
	}

	public void setWhen(String when) {
		this.when = when;
	}

	public String getCatalog() {
		return catalog;
	}

	public void setCatalog(String catalog) {
		this.catalog = catalog;
	}

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public String getDataSourceFilter() {
		return dataSourceFilter;
	}

	public void setDataSourceFilter(String dataSourceFilter) {
		this.dataSourceFilter = dataSourceFilter;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getBindTableName() {
		return bindTableName;
	}

	public void setBindTableName(String bindTableName) {
		this.bindTableName = bindTableName;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public Boolean getSmart() {
		return smart;
	}

	public void setSmart(Boolean smart) {
		this.smart = smart;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

}
