package cn.tenmg.clink.jdbc.getter;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * java.lang.Object类型结果获取器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public class ObjectResultGetter extends AbstractResultGetter<Object> {

	@Override
	public Object getValue(ResultSet rs, int columnIndex) throws SQLException {
		return rs.getObject(columnIndex);
	}

	@Override
	public Object getValue(ResultSet rs, String columnLabel) throws SQLException {
		return rs.getObject(columnLabel);
	}

}
