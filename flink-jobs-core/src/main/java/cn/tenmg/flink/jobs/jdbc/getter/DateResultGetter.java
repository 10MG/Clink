package cn.tenmg.flink.jobs.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * {@link java.util.Date} 类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class DateResultGetter extends AbstractResultGetter<Date> {

	@Override
	public Date getValue(ResultSet rs, int columnIndex) throws SQLException {
		Timestamp t = rs.getTimestamp(columnIndex);
		return t == null ? null : new Date(t.getTime());
	}

	@Override
	public Date getValue(ResultSet rs, String columnLabel) throws SQLException {
		Timestamp t = rs.getTimestamp(columnLabel);
		return t == null ? null : new Date(t.getTime());
	}

}
