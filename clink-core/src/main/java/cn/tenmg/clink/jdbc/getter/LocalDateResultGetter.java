package cn.tenmg.clink.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

/**
 * 
 * java.time.LocalDate类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class LocalDateResultGetter extends AbstractResultGetter<LocalDate> {

	@Override
	public LocalDate getValue(ResultSet rs, int columnIndex) throws SQLException {
		return (LocalDate) rs.getObject(columnIndex);
	}

	@Override
	public LocalDate getValue(ResultSet rs, String columnLabel) throws SQLException {
		return (LocalDate) rs.getObject(columnLabel);
	}

}
