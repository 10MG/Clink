package cn.tenmg.flink.jobs.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

/**
 * 属性工具类。已废弃
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.0.0
 */
@Deprecated
public abstract class FieldUtils {

	public static final void parseFields(Map<String, Integer> feildNames, Map<Integer, Field> fieldMap,
			Field[] fields) {
		for (int i = 0; i < fields.length; i++) {
			Field field = fields[i];
			String name = field.getName();
			Integer index = feildNames.get(name);
			if (index == null) {
				index = feildNames.get(name.toLowerCase());
				if (index != null) {
					setFieldAccessible(fieldMap, field, index);
				}
			} else {
				setFieldAccessible(fieldMap, field, index);
			}
		}
	}

	public static final void setFieldAccessible(Map<Integer, Field> fieldMap, Field field, int columnIndex) {
		if (!fieldMap.containsKey(columnIndex) && !Modifier.isFinal(field.getModifiers())) {
			fieldMap.put(columnIndex, field);
			field.setAccessible(true);
		}
	}
}
