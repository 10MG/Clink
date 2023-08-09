package cn.tenmg.clink.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link java.lang.Byte} 类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class ByteResultGetter extends AbstractResultGetter<Byte> {

	@Override
	public Byte getValue(ResultSet rs, int columnIndex) throws SQLException {
		return toByte(rs.getObject(columnIndex));
	}

	@Override
	public Byte getValue(ResultSet rs, String columnLabel) throws SQLException {
		return toByte(rs.getObject(columnLabel));
	}

	private static Byte toByte(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof Byte) {
			return (Byte) value;
		}
		return Byte.valueOf(value.toString());
	}

}
