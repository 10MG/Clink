package cn.tenmg.flink.jobs.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@code byte[]} 类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class BytesResultGetter extends AbstractResultGetter<byte[]> {

	@Override
	public byte[] getValue(ResultSet rs, int columnIndex) throws SQLException {
		return rs.getBytes(columnIndex);
	}

	@Override
	public byte[] getValue(ResultSet rs, String columnLabel) throws SQLException {
		return rs.getBytes(columnLabel);
	}
}
