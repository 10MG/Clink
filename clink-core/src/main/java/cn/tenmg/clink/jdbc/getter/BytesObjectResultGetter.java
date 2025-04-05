package cn.tenmg.clink.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link java.lang.Byte}{@code []} 类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class BytesObjectResultGetter extends AbstractResultGetter<Byte[]> {

	@Override
	public Byte[] getValue(ResultSet rs, int columnIndex) throws SQLException {
		return toBytes(rs.getObject(columnIndex));
	}

	@Override
	public Byte[] getValue(ResultSet rs, String columnLabel) throws SQLException {
		return toBytes(rs.getObject(columnLabel));
	}

	private static Byte[] toBytes(Object value) {
		if (value == null) {
			return null;
		} else {
			return (Byte[]) value;
		}
	}

}
