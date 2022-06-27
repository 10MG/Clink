package cn.tenmg.flink.jobs.metadata.getter;

import java.sql.Connection;
import java.util.Map;

import cn.tenmg.flink.jobs.utils.JDBCUtils;

/**
 * JDBC元数据获取器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
public class JDBCMetaDataGetter extends AbstractJDBCMetaDataGetter {

	@Override
	Connection getConnection(Map<String, String> dataSource) throws Exception {
		return JDBCUtils.getConnection(dataSource);
	}

}