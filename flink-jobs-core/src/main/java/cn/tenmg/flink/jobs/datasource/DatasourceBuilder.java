package cn.tenmg.flink.jobs.datasource;

import java.util.Properties;

import javax.sql.DataSource;

/**
 * 数据源构建器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.4.0
 */
public interface DatasourceBuilder {

	DataSource createDataSource(Properties properties) throws Exception;

}
