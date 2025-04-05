package cn.tenmg.clink.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * java.lang.String类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class StringResultGetter extends AbstractResultGetter<String> {

	@Override
	public String getValue(ResultSet rs, int columnIndex) throws SQLException {
		return rs.getString(columnIndex);
	}

	@Override
	public String getValue(ResultSet rs, String columnLabel) throws SQLException {
		return rs.getString(columnLabel);
	}

}
