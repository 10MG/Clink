package cn.tenmg.flink.jobs.utils;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * 对象工具类。已废弃，将在下一版本移除，请使用cn.tenmg.dsl.utils.ObjectUtils替换
 * 
 * @author 赵伟均 wjzhao@aliyun.com
 *
 */
@Deprecated
public abstract class ObjectUtils {

	private static volatile Map<Class<?>, Map<String, Field>> fieldMap = new HashMap<Class<?>, Map<String, Field>>();

	/**
	 * 获取指定对象中的指定成员变量
	 * 
	 * @param object
	 *            指定对象
	 * @param fieldName
	 *            指定成员变量
	 * @param <T>
	 *            返回类型
	 * @return 返回指定成员变量的值
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getValue(Object object, String fieldName) {
		T value = null;
		if (object instanceof Map) {
			value = (T) ((Map<?, ?>) object).get(fieldName);
		} else {
			Class<?> cls = object.getClass();
			Map<String, Field> fields = fieldMap.get(cls);
			if (fields == null) {
				synchronized (ObjectUtils.class) {
					fields = fieldMap.get(cls);
					if (fields == null) {
						fields = new HashMap<String, Field>();
						while (!cls.equals(Object.class)) {
							Field[] declaredFields = cls.getDeclaredFields();
							for (int i = 0; i < declaredFields.length; i++) {
								Field field = declaredFields[i];
								fields.put(field.getName(), field);
							}
							cls = cls.getSuperclass();
						}
						fieldMap.put(cls, fields);
					}
				}
			}
			Field field = fields.get(fieldName);
			if (field != null) {
				if (field.isAccessible()) {
					try {
						value = (T) field.get(object);
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else {
					field.setAccessible(true);
					try {
						value = (T) field.get(object);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
		return value;
	}

}
