package cn.tenmg.flink.jobs.jdbc.executor;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.tenmg.dsl.utils.StringUtils;
import cn.tenmg.flink.jobs.jdbc.exception.SQLExecutorException;
import cn.tenmg.flink.jobs.utils.FieldUtils;

/**
 * 查询单条记录的数据的SQL执行器
 * 
 * @author June wjzhao@aliyun.com
 *
 * @param <T>
 *            实体类
 *
 * @since 1.4.0
 */
public class GetSQLExecutor<T> extends ReadOnlySQLExecutor<T> {

	protected Class<T> type;

	@SuppressWarnings("unchecked")
	protected GetSQLExecutor() {
		type = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
	}

	public GetSQLExecutor(Class<T> type) {
		this.type = type;
	}

	@Override
	public boolean isReadOnly() {
		return true;
	}

	@Override
	public ResultSet executeQuery(PreparedStatement ps) throws SQLException {
		return ps.executeQuery();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public T execute(PreparedStatement ps, ResultSet rs) throws SQLException {
		T row = null;
		if (rs.next()) {
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			if (Map.class.isAssignableFrom(type)) {
				List<String> feildNames = new ArrayList<String>();
				for (int i = 1; i <= columnCount; i++) {
					feildNames.add(StringUtils.toCamelCase(rsmd.getColumnLabel(i), "_", false));
				}
				try {
					row = type.newInstance();
					for (int i = 1; i <= columnCount; i++) {
						((Map) row).put(feildNames.get(i - 1), rs.getObject(i));
					}
				} catch (InstantiationException | IllegalAccessException e) {
					throw new SQLExecutorException(e);
				}
			} else if (BigDecimal.class.isAssignableFrom(type)) {
				row = (T) rs.getBigDecimal(1);
			} else if (Number.class.isAssignableFrom(type)) {
				Object obj = rs.getObject(1);
				if (obj == null) {
					return null;
				}
				if (obj instanceof Number) {
					if (Double.class.isAssignableFrom(type)) {
						obj = ((Number) obj).doubleValue();
					} else if (Float.class.isAssignableFrom(type)) {
						obj = ((Number) obj).floatValue();
					} else if (Integer.class.isAssignableFrom(type)) {
						obj = ((Number) obj).intValue();
					} else if (Long.class.isAssignableFrom(type)) {
						obj = ((Number) obj).longValue();
					} else if (Short.class.isAssignableFrom(type)) {
						obj = ((Number) obj).shortValue();
					} else if (Byte.class.isAssignableFrom(type)) {
						obj = ((Number) obj).byteValue();
					}
				}
				row = (T) obj;
			} else if (String.class.isAssignableFrom(type)) {
				row = (T) rs.getString(1);
			} else if (Date.class.isAssignableFrom(type)) {
				row = (T) rs.getObject(1);
			} else if (List.class.isAssignableFrom(type)) {
				try {
					row = type.newInstance();
					for (int i = 1; i <= columnCount; i++) {
						((List) row).add(rs.getObject(i));
					}
				} catch (InstantiationException | IllegalAccessException e) {
					throw new SQLExecutorException(e);
				}
			} else {
				Map<String, Integer> feildNames = new HashMap<String, Integer>();
				for (int i = 1; i <= columnCount; i++) {
					String feildName = StringUtils.toCamelCase(rsmd.getColumnLabel(i), "_", false);
					feildNames.put(feildName, i);
				}
				Map<Integer, Field> fieldMap = new HashMap<Integer, Field>();
				Class<?> current = type;
				while (!Object.class.equals(current)) {
					FieldUtils.parseFields(feildNames, fieldMap, current.getDeclaredFields());
					current = current.getSuperclass();
				}
				try {
					row = type.newInstance();
					for (int i = 1; i <= columnCount; i++) {
						Field field = fieldMap.get(i);
						if (field != null) {
							field.set(row, getValue(rs, i, field.getType()));
						}
					}
				} catch (InstantiationException | IllegalAccessException e) {
					throw new SQLExecutorException(e);
				}
			}
		}
		if (rs.next()) {
			throw new SQLExecutorException(
					"Statement returned more than one row, where no more than one was expected.");
		}
		return row;
	}

}
