package cn.tenmg.clink.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;

/**
 * java.sql.Time类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class TimeResultGetter extends AbstractResultGetter<Time> {

	@Override
	public Time getValue(ResultSet rs, int columnIndex) throws SQLException {
		return rs.getTime(columnIndex);
	}

	@Override
	public Time getValue(ResultSet rs, String columnLabel) throws SQLException {
		return rs.getTime(columnLabel);
	}

}
