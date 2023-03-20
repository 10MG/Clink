package cn.tenmg.flink.jobs.jdbc.executor;

import java.lang.reflect.ParameterizedType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 查询记录列表的SQL执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @param <T>
 *            实体类
 *
 * @since 1.4.0
 */
public class SelectSQLExecutor<T> extends ReadOnlySQLExecutor<List<T>> {

	protected Class<T> type;

	@SuppressWarnings("unchecked")
	protected SelectSQLExecutor() {
		type = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
	}

	public SelectSQLExecutor(Class<T> type) {
		this.type = type;
	}

	@Override
	public ResultSet executeQuery(PreparedStatement ps) throws SQLException {
		return ps.executeQuery();
	}

	@Override
	public List<T> execute(PreparedStatement ps, ResultSet rs) throws SQLException {
		List<T> rows = new ArrayList<T>();
		while (rs.next()) {
			rows.add(getRow(rs, type));
		}
		return rows;
	}

}
