package cn.tenmg.flink.jobs.datasource.builder.org.apache.commons.dbcp2;

import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSourceFactory;

import cn.tenmg.flink.jobs.datasource.DatasourceBuilder;

/**
 * DBCP2数据源构建器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.2.0
 *
 */
public class BasicDataSourceBuilder implements DatasourceBuilder {

	@Override
	public DataSource createDataSource(Properties properties) throws Exception {
		return BasicDataSourceFactory.createDataSource(properties);
	}

}
