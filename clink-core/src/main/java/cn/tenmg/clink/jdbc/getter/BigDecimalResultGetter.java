package cn.tenmg.clink.jdbc.getter;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * {@link java.math.BigDecimal} 类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.5.6
 */
public class BigDecimalResultGetter extends AbstractResultGetter<BigDecimal> {

	@Override
	public BigDecimal getValue(ResultSet rs, int columnIndex) throws SQLException {
		return rs.getBigDecimal(columnIndex);
	}

	@Override
	public BigDecimal getValue(ResultSet rs, String columnLabel) throws SQLException {
		return rs.getBigDecimal(columnLabel);
	}
}
