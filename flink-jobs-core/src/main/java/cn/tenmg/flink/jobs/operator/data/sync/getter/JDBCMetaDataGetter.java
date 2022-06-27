package cn.tenmg.flink.jobs.operator.data.sync.getter;

import java.sql.Connection;
import java.util.Map;

import cn.tenmg.flink.jobs.utils.JDBCUtils;

/**
 * JDBC元数据获取器。已废弃，请使用cn.tenmg.flink.jobs.metadata.getter.JDBCMetaDataGetter替代
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.1.2
 */
@Deprecated
public class JDBCMetaDataGetter extends AbstractJDBCMetaDataGetter {

	@Override
	Connection getConnection(Map<String, String> dataSource) throws Exception {
		return JDBCUtils.getConnection(dataSource);
	}

}