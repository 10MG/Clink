package cn.tenmg.flink.jobs.jdbc.getter;

import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * java.net.URL类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public class URLResultGetter extends AbstractResultGetter<URL> {

	@Override
	public URL getValue(ResultSet rs, int columnIndex) throws SQLException {
		return rs.getURL(columnIndex);
	}

	@Override
	public URL getValue(ResultSet rs, String columnLabel) throws SQLException {
		return rs.getURL(columnLabel);
	}

}
