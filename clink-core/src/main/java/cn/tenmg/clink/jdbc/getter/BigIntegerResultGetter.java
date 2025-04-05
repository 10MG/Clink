package cn.tenmg.clink.jdbc.getter;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link java.math.BigInteger} 类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class BigIntegerResultGetter extends AbstractResultGetter<BigInteger> {

	@Override
	public BigInteger getValue(ResultSet rs, int columnIndex) throws SQLException {
		return toBigInteger(rs.getObject(columnIndex));
	}

	@Override
	public BigInteger getValue(ResultSet rs, String columnLabel) throws SQLException {
		return toBigInteger(rs.getObject(columnLabel));
	}

	private static BigInteger toBigInteger(Object value) {
		if (value == null) {
			return null;
		} else if (value instanceof BigInteger) {
			return (BigInteger) value;
		} else if (value instanceof Long) {
			return BigInteger.valueOf((long) value);
		} else {
			return BigInteger.valueOf(Long.valueOf(value.toString()));
		}
	}

}
