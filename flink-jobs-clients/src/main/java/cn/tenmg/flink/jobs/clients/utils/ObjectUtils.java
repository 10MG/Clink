package cn.tenmg.flink.jobs.clients.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 对象工具类
 * 
 * @author June wjzhao@aliyun.com
 *
 * @since 1.5.6
 */
public abstract class ObjectUtils {

	private static final Pattern ARRAY_PATTERN = Pattern.compile("\\[[^\\]]+\\]"),
			ARRAY_END_PATTERN = Pattern.compile("\\[[^\\]]+\\]$");

	private static final String GET = "get", SET = "set", METHOD_RETURNTYPE_SPLITOR = ":";

	private static volatile Map<Class<?>, Map<String, Field>> fieldMap = new HashMap<Class<?>, Map<String, Field>>(128);

	private static volatile Map<Class<?>, Map<String, Method>> getMethodMap = new HashMap<Class<?>, Map<String, Method>>(
			128);

	private static volatile Map<Class<?>, Map<String, List<Method>>> setMethodMap = new HashMap<Class<?>, Map<String, List<Method>>>(
			128);

	private static volatile Map<Class<?>, Map<String, Method>> bestSetMethodMap = new HashMap<Class<?>, Map<String, Method>>(
			128);

	private ObjectUtils() {
	}

	/**
	 * 根据属性表达式获取对象的属性值。属性表达式支持使用“field1.field2”访问属性的属性值，层级数不限，支持使用“[*]”访问数组值，维数不限，“field1.field2”和“[*]”也可以配合使用。
	 * 
	 * @param object
	 *            获取的对象
	 * @param attribute
	 *            属性表达式
	 * @return 根据属性表达式获取，如果对象的指定属性（或者子孙属性）存在，则返回它的值；否则，返回null。
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getValue(Object object, String attribute) throws Exception {
		Object value = getValueInner(object, attribute);
		if (value == null) {
			if (attribute.contains(".")) {// 访问Bean或者Map属性
				String[] names = attribute.split("\\.");
				attribute = names[0];
				value = getValueInner(object, attribute);
				if (value == null) {// 如果类似“key.name[*]……”形式的，可能是访问数组的某一项值或者是访问Map对象的属性值。如果是，则获取数组的某一项值或者Map对象的某个属性值
					return getMaybeArrayOrMapValue(object, attribute);
				} else {
					for (int i = 1; i < names.length; i++) {
						attribute = names[i];
						value = getValue(value, attribute);// 获取对象属性
						if (value == null) {// 可能是数组
							Matcher m = ARRAY_PATTERN.matcher(attribute);
							if (m.find()) {// 含有数组访问符“[]”
								value = getValue(value, attribute.substring(0, attribute.indexOf("[")));// 获取数组对象
								if (value == null) {// 数组对象为null，返回null
									return null;
								} else {// 否则，获取数组的值
									value = getArrayOrMapValue(value, object, m);
								}
							}
							return (T) value;
						}
					}
					return (T) value;
				}
			} else {// 如果类似“key[*]……”形式的，可能是访问数组的某一项值或者是访问Map对象的属性值。如果是，则获取数组的某一项值或者Map对象的某个属性值
				return getMaybeArrayOrMapValue(object, attribute);
			}
		} else {
			return (T) value;
		}
	}

	/**
	 * 根据属性表达式获取对象的属性值，并忽略异常，当异常发生时，返回 <code>null</code> 。
	 * 
	 * @param object
	 *            获取的对象
	 * @param attribute
	 *            属性表达式。支持使用“field1.field2”访问属性的属性值，层级数不限，支持使用“[*]”访问数组值，维数不限，“field1.field2”和“[*]”也可以配合使用
	 * @param <T>
	 *            返回类型
	 * @return 根据属性表达式获取，如果对象的指定属性（或者子孙属性）存在，则返回它的值；否则，返回null。
	 */
	public static <T> T getValueIgnoreException(Object object, String attribute) {
		T value = null;
		try {
			value = getValue(object, attribute);
		} catch (Exception e) {
		}
		return value;
	}

	/**
	 * 获取设置指定对象的指定字段时所需的类型。类型的优先顺序为setter方法的参数类型 > 字段类型。
	 * 
	 * @param object
	 *            指定对象
	 * @param fieldName
	 *            指定字段名
	 * @return 设置指定对象的指定字段时所需的类型
	 */
	public static Class<?> getFieldType(Object object, String fieldName) {
		return getFieldType(object, fieldName, true);
	}

	/**
	 * 获取设置指定对象的指定字段时所需的类型。类型的优先顺序为setter方法的参数类型 > 字段类型。
	 * 
	 * @param object
	 *            指定对象
	 * @param fieldName
	 *            指定字段名
	 * @param throwWhenAbsent
	 *            指定字段不存在是否抛出异常
	 * @return 设置指定对象的指定字段时所需的类型
	 */
	public static Class<?> getFieldType(Object object, String fieldName, boolean throwWhenAbsent) {
		Class<?> type = object.getClass();
		String methodName = toMethodName(SET, fieldName);
		List<Method> methods = getSetMethods(type).get(methodName);
		if (methods == null) {
			Field field = getFields(type).get(fieldName);
			if (field == null) {
				if (throwWhenAbsent) {
					throw new IllegalArgumentException(StringUtils.concat("There is no suitable method named ",
							methodName, " or field named ", fieldName, " in class ", type.getName()));
				}
				return null;
			} else {
				return field.getType();
			}
		} else {
			int size = methods.size();
			if (size == 1) {
				return methods.get(0).getParameterTypes()[0];
			} else {
				Field field = getFields(type).get(fieldName);
				if (field == null) {
					if (throwWhenAbsent) {
						throw new IllegalArgumentException(StringUtils.concat("There is no field named ", fieldName,
								" and more than one set method named ", methodName, " in class ", type.getName(),
								", we don't know which one to use for getting field type"));
					}
					return null;
				} else {
					return field.getType();
				}
			}
		}
	}

	/**
	 * 根据属性表达式设置指定对象属性的值。属性表达式支持使用“field1.field2”访问属性的属性值，层级数不限，支持使用“[*]”访问数组值，维数不限，“field1.field2”和“[*]”也可以配合使用。如果设置的是孙子属性，则如果孙子的父属性不存在则会默认根据setter方法或者属性类型自动设置父属性后，再设置孙子属性。以此类推。
	 * 
	 * @param object
	 *            指定对象
	 * @param attribute
	 *            属性表达式
	 * @param value
	 *            指定的值
	 * @throws Exception
	 *             发生异常
	 */
	public static <T> void setValue(Object object, String attribute, T value) throws Exception {
		setValue(object, attribute, value, true);
	}

	/**
	 * 根据属性表达式设置指定对象属性的值。属性表达式支持使用“field1.field2”访问属性的属性值，层级数不限，支持使用“[*]”访问数组值，维数不限，“field1.field2”和“[*]”也可以配合使用。如果设置的是孙子属性，则如果孙子的父属性不存在则会默认根据setter方法或者属性类型自动设置父属性后，再设置孙子属性。以此类推。
	 * 
	 * @param object
	 *            指定对象
	 * @param attribute
	 *            属性表达式
	 * @param value
	 *            指定的值
	 * @param throwWhenAbsent
	 *            指定属性不存在是否抛出异常
	 * @throws Exception
	 *             发生异常
	 */
	public static <T> void setValue(Object object, String attribute, T value, boolean throwWhenAbsent)
			throws Exception {
		Matcher matcher = ARRAY_END_PATTERN.matcher(attribute);
		if (matcher.find()) {// 以“[*]”结尾的
			String group = matcher.group();
			setValue(object, attribute.substring(0, attribute.length() - group.length()),
					group.substring(1, group.length() - 1), value, throwWhenAbsent);
		} else {
			int index = attribute.lastIndexOf(".");
			if (index > 0) {
				setValue(object, attribute.substring(0, index), attribute.substring(index + 1), value, throwWhenAbsent);
			} else {
				setValueInner(object, attribute, value, throwWhenAbsent);
			}
		}
	}

	/**
	 * 获取指定对象中的指定字段
	 * 
	 * @param object
	 *            指定对象
	 * @param fieldName
	 *            指定字段
	 * @param <T>
	 *            返回类型
	 * @return 返回指定字段的值
	 * @throws Exception
	 *             发生异常
	 */
	private static Object getValueInner(Object object, String fieldName) throws Exception {
		if (object instanceof Map) {
			return ((Map<?, ?>) object).get(fieldName);
		} else if (object instanceof List) {
			return ((List<?>) object).get(toIndex(fieldName));
		} else if (object instanceof Object[]) {
			return ((Object[]) object)[toIndex(fieldName)];
		} else if (object instanceof int[]) {
			return ((int[]) object)[toIndex(fieldName)];
		} else if (object instanceof long[]) {
			return ((long[]) object)[toIndex(fieldName)];
		} else if (object instanceof double[]) {
			return ((double[]) object)[toIndex(fieldName)];
		} else if (object instanceof float[]) {
			return ((float[]) object)[toIndex(fieldName)];
		} else if (object instanceof short[]) {
			return ((short[]) object)[toIndex(fieldName)];
		} else if (object instanceof char[]) {
			return ((char[]) object)[toIndex(fieldName)];
		} else if (object instanceof boolean[]) {
			return ((boolean[]) object)[toIndex(fieldName)];
		} else if (object instanceof byte[]) {
			return ((byte[]) object)[toIndex(fieldName)];
		} else if (object instanceof LinkedHashSet) {
			return ((LinkedHashSet<?>) object).toArray()[toIndex(fieldName)];
		} else {
			Class<?> type = object.getClass();
			Method method = getGetMethods(type).get(toMethodName(GET, fieldName));
			if (method == null) {
				Field field = getFields(type).get(fieldName);
				if (field == null) {
					return null;
				} else {
					return field.get(object);
				}
			} else {
				return method.invoke(object);
			}
		}
	}

	/**
	 * 将索引表达式转换为索引值
	 * 
	 * @param expr
	 *            索引表达式
	 * @return 索引值
	 */
	private static int toIndex(String expr) {
		if (expr.startsWith("[") && expr.endsWith("]")) {
			return Integer.valueOf(expr.substring(1, expr.length() - 1).trim());
		}
		return Integer.valueOf(expr.trim());
	}

	/**
	 * 将属性名转换为getter/setter方法名
	 * 
	 * @param prefix
	 *            方法名前缀
	 * @param fieldName
	 *            属性名
	 * @return getter/setter方法名
	 */
	private static String toMethodName(String prefix, String fieldName) {
		return prefix + (fieldName.length() > 1 ? fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1)
				: fieldName.toUpperCase());
	}

	/**
	 * 获取指定类的获取方法（getter）集
	 * 
	 * @param type
	 *            指定类
	 * @return 获取方法（getter）集
	 */
	private static Map<String, Method> getGetMethods(Class<?> type) {
		Map<String, Method> getMethods = getMethodMap.get(type);
		if (getMethods == null) {
			synchronized (getMethodMap) {
				getMethods = getMethodMap.get(type);
				if (getMethods == null) {
					getMethods = new HashMap<String, Method>(128);
					Method[] mthds = type.getMethods();
					String name;
					for (int i = 0, count; i < mthds.length; i++) {
						Method method = mthds[i];
						name = method.getName();
						count = method.getParameterCount();
						if (count == 0) {
							if (name.startsWith(GET)) {
								getMethods.put(name, method);
							} else if (name.startsWith("is") && name.length() > 2
									&& Character.isUpperCase(name.charAt(2))
									&& Boolean.class.isAssignableFrom(method.getReturnType())) {
								getMethods.put(GET.concat(name.substring(2)), method);
							} else {
								getMethods.put(GET.concat(name), method);
							}
						}
					}
					getMethodMap.put(type, getMethods);
				}
			}
		}
		return getMethods;
	}

	/**
	 * 获取指定类的设置方法（setter）集
	 * 
	 * @param type
	 *            指定类
	 * @return 设置方法（setter）集
	 */
	private static Map<String, List<Method>> getSetMethods(Class<?> type) {
		Map<String, List<Method>> methods = setMethodMap.get(type);
		if (methods == null) {
			synchronized (setMethodMap) {
				methods = setMethodMap.get(type);
				if (methods == null) {
					methods = new HashMap<String, List<Method>>(128);
					Method[] mthds = type.getMethods();
					String name;
					List<Method> setMethods;
					for (int i = 0, count; i < mthds.length; i++) {
						Method method = mthds[i];
						name = method.getName();
						count = method.getParameterCount();
						if (count == 1 && name.startsWith("set")) {
							setMethods = methods.get(name);
							if (setMethods == null) {
								setMethods = new ArrayList<Method>();
								methods.put(name, setMethods);
							}
							setMethods.add(method);
						}
					}
					setMethodMap.put(type, methods);
				}
			}
		}
		return methods;
	}

	/**
	 * 获取指定类型指定字段的最佳匹配设置方法（setter）
	 * 
	 * @param type
	 *            指定类型
	 * @param setMethods
	 *            对象设置方法（setter）集
	 * @param methodName
	 *            设置方法名
	 * @param value
	 *            设置的值
	 * @return 最佳匹配设置方法（setter）
	 */
	private static <T> Method getBestSetMethod(Class<?> type, List<Method> setMethods, String methodName, T value) {
		Map<String, Method> bestSetMethods = bestSetMethodMap.get(type);
		if (bestSetMethods == null) {
			synchronized (bestSetMethodMap) {
				bestSetMethods = bestSetMethodMap.get(type);
				if (bestSetMethods == null) {
					bestSetMethods = new HashMap<String, Method>(128);
					bestSetMethodMap.put(type, bestSetMethods);
				}
				return getBestSetMethod(bestSetMethods, setMethods, methodName, value);
			}
		} else {
			synchronized (bestSetMethods) {
				return getBestSetMethod(bestSetMethods, setMethods, methodName, value);
			}
		}
	}

	private static <T> Method getBestSetMethod(Map<String, Method> bestSetMethods, List<Method> setMethods,
			String methodName, T value) {
		Method bestSetMethod = null;
		if (value == null) {
			if (bestSetMethods.containsKey(methodName)) {
				bestSetMethod = bestSetMethods.get(methodName);
			} else {
				Method curMethod;
				for (int i = 0, size = setMethods.size(); i < size; i++) {
					curMethod = setMethods.get(i);
					if (Object.class.isAssignableFrom(curMethod.getParameterTypes()[0])) {
						bestSetMethod = curMethod;
					}
				}
				bestSetMethods.put(methodName, bestSetMethod);
			}
		} else {
			Class<?> valueType = value.getClass();
			String bestKey = String.join(METHOD_RETURNTYPE_SPLITOR, methodName, valueType.getName());
			if (bestSetMethods.containsKey(bestKey)) {
				bestSetMethod = bestSetMethods.get(bestKey);
			} else {
				Method curMethod;
				Class<?> parameterType;
				for (int i = 0, minGeneration = Integer.MAX_VALUE, generation, size = setMethods
						.size(); i < size; i++) {
					curMethod = setMethods.get(i);
					parameterType = curMethod.getParameterTypes()[0];
					if (parameterType.equals(valueType)) {
						bestSetMethod = curMethod;
						break;
					} else if (parameterType.isAssignableFrom(valueType)) {
						generation = 1;
						while (!parameterType.equals(valueType)) {
							valueType = valueType.getSuperclass();
							generation++;
						}
						if (generation < minGeneration) {
							bestSetMethod = curMethod;
						}
					}
				}
				bestSetMethods.put(bestKey, bestSetMethod);
			}
		}
		return bestSetMethod;
	}

	/**
	 * 获取指定类行的字段名为键字段为值的对照表。
	 * 
	 * @param type
	 *            指定类型
	 * @return 字段名为键字段为值的对照表
	 */
	private static Map<String, Field> getFields(Class<?> type) {
		Map<String, Field> fields = fieldMap.get(type);
		if (fields == null) {
			synchronized (fieldMap) {
				fields = fieldMap.get(type);
				if (fields == null) {
					fields = new HashMap<String, Field>();
					while (!type.equals(Object.class)) {
						Field[] declaredFields = type.getDeclaredFields();
						for (int i = 0; i < declaredFields.length; i++) {
							Field field = declaredFields[i];
							if (!field.isAccessible() && !Modifier.isFinal(field.getModifiers())) {
								field.setAccessible(true);
							}
							fields.put(field.getName(), field);
						}
						type = type.getSuperclass();
					}
					fieldMap.put(type, fields);
				}
			}
		}
		return fields;
	}

	/**
	 * 获取可能通过“[*]”方式访问的数组某一项值或Map对象属性值
	 * 
	 * @param object
	 *            获取的对象
	 * @param attribute
	 *            属性表达式
	 * @return 如果含有“[*]”符号，则获取并返回数组的值或者Map对象的属性值；否则返回null。
	 * @throws Exception
	 *             发生异常
	 */
	@SuppressWarnings("unchecked")
	private static final <T> T getMaybeArrayOrMapValue(Object object, String attribute) throws Exception {
		Object value = null;
		Matcher m = ARRAY_PATTERN.matcher(attribute);
		if (m.find()) {
			String parentAttribute = attribute.substring(0, attribute.indexOf("["));
			if (parentAttribute.isEmpty()) {
				value = object;
			} else {
				value = getValueInner(object, parentAttribute);
			}
			if (value == null) {
				return null;
			} else {// 继续获取下一维数组的值
				value = getArrayOrMapValue(value, object, m);
			}
		}
		return (T) value;
	}

	@SuppressWarnings("unchecked")
	private static final <T> T getArrayOrMapValue(Object value, Object object, Matcher m) throws Exception {
		value = getArrayOrMapValue(value, object, m.group());
		while (value != null && m.find()) {
			value = getArrayOrMapValue(value, object, m.group());
		}
		return (T) value;
	}

	private static final Object getArrayOrMapValue(Object value, Object object, String group) throws Exception {
		String attribute = group.substring(1, group.length() - 1);
		Object v = getValueInner(object, attribute);
		String key = attribute;
		if (v != null) {
			key = v.toString();
		}
		if (value instanceof Map) {
			return ((Map<?, ?>) value).get(key);
		} else if (value instanceof List) {
			return ((List<?>) value).get(Integer.valueOf(key));
		} else if (value instanceof Object[]) {
			return ((Object[]) value)[Integer.valueOf(key)];
		} else if (value instanceof int[]) {
			return ((int[]) value)[Integer.valueOf(key)];
		} else if (value instanceof long[]) {
			return ((long[]) value)[Integer.valueOf(key)];
		} else if (value instanceof double[]) {
			return ((double[]) value)[Integer.valueOf(key)];
		} else if (value instanceof float[]) {
			return ((float[]) value)[Integer.valueOf(key)];
		} else if (value instanceof short[]) {
			return ((short[]) value)[Integer.valueOf(key)];
		} else if (value instanceof char[]) {
			return ((char[]) value)[Integer.valueOf(key)];
		} else if (value instanceof boolean[]) {
			return ((boolean[]) value)[Integer.valueOf(key)];
		} else if (value instanceof byte[]) {
			return ((byte[]) value)[Integer.valueOf(key)];
		} else if (value instanceof LinkedHashSet) {
			return ((LinkedHashSet<?>) value).toArray()[Integer.valueOf(key)];
		} else {
			return null;
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void setValue(Object object, String parentAttribute, String attribute, Object value,
			boolean throwWhenAbsent) throws Exception {
		if (StringUtils.isBlank(parentAttribute)) {
			setValueInner(object, attribute, value, throwWhenAbsent);
		} else {
			Object parent = getValue(object, parentAttribute);
			if (parent == null) {
				if (object instanceof Map) {
					((Map) object).put(attribute, value);
				} else if (object instanceof List) {
					int index = Integer.valueOf(attribute);
					List<Object> list = ((List<Object>) object);
					while (list.size() <= index) {
						list.add(null);
					}
					list.set(index, value);
				} else if (object instanceof Object[]) {
					((Object[]) object)[Integer.valueOf(attribute)] = value;
				} else if (object instanceof int[]) {
					((int[]) object)[Integer.valueOf(attribute)] = (int) value;
				} else if (object instanceof long[]) {
					((long[]) object)[Integer.valueOf(attribute)] = (long) value;
				} else if (object instanceof double[]) {
					((double[]) object)[Integer.valueOf(attribute)] = (double) value;
				} else if (object instanceof float[]) {
					((float[]) object)[Integer.valueOf(attribute)] = (float) value;
				} else if (object instanceof short[]) {
					((short[]) object)[Integer.valueOf(attribute)] = (short) value;
				} else if (object instanceof char[]) {
					((char[]) object)[Integer.valueOf(attribute)] = (char) value;
				} else if (object instanceof boolean[]) {
					((boolean[]) object)[Integer.valueOf(attribute)] = (boolean) value;
				} else if (object instanceof byte[]) {
					((byte[]) object)[Integer.valueOf(attribute)] = (byte) value;
				} else if (object instanceof LinkedHashSet) {
					LinkedHashSet<Object> set = ((LinkedHashSet<Object>) object);
					int index = Integer.valueOf(attribute), size = set.size();
					if (index < size) {
						Object[] elms = set.toArray();
						set.clear();
						for (int i = 0; i < index; i++) {
							set.add(elms[i]);
						}
						set.add(value);
						for (int i = index; i < size; i++) {
							set.add(elms[i]);
						}
					} else if (index == size) {
						set.add(value);
					} else {
						if (size > 1) {
							throw new IllegalArgumentException(
									StringUtils.concat("Trying to add an element at the index ", index,
											", but there is only ", size, " elements in ", set.toString()));
						} else {
							throw new IllegalArgumentException(StringUtils.concat(
									"Trying to add an element at the index ", index, ", but there is ",
									(size > 0 ? "only 1" : "no"), " element in ", set.toString()));
						}
					}
				} else {
					parent = getParent(object, parentAttribute, throwWhenAbsent);
					setValueInner(parent, attribute, value, throwWhenAbsent);
				}
			} else {
				setValueInner(parent, attribute, value, throwWhenAbsent);
			}
		}
	}

	/**
	 * 设置指定对象中的指定字段的值。
	 * 
	 * @param object
	 *            指定对象
	 * @param fieldName
	 *            指定字段
	 * @param value
	 *            指定的值
	 * @param throwWhenAbsent
	 *            指定字段不存在则抛出异常
	 * @throws Exception
	 *             发生异常
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void setValueInner(Object object, String fieldName, Object value, boolean throwWhenAbsent)
			throws Exception {
		if (object instanceof Map) {
			((Map) object).put(fieldName, value);
		} else {
			Class<?> type = object.getClass();
			String methodName = toMethodName(SET, fieldName);
			List<Method> methods = getSetMethods(type).get(methodName);
			if (methods == null) {
				Field field = getFields(type).get(fieldName);
				if (field == null) {
					if (throwWhenAbsent) {
						throw new IllegalArgumentException(StringUtils.concat("There is no suitable method named ",
								methodName, " or field named ", fieldName, " in class ", type.getName()));
					}
				} else {
					field.set(object, value);
				}
			} else {
				int size = methods.size();
				if (size == 1) {
					methods.get(0).invoke(object, value);
				} else {
					Method method = getBestSetMethod(type, methods, methodName, value);
					if (method == null) {
						Field field = getFields(type).get(fieldName);
						if (field == null) {
							if (throwWhenAbsent) {
								throw new IllegalArgumentException(
										StringUtils.concat("There is no suitable method named ", methodName,
												" or field named ", fieldName, " in class ", type.getName()));
							}
						} else {
							field.set(object, value);
						}
					} else {
						method.invoke(object, value);
					}
				}
			}
		}
	}

	private static Object getParent(Object object, String attribute, boolean throwWhenAbsent) throws Exception {
		Object parent = object;
		Matcher matcher = ARRAY_END_PATTERN.matcher(attribute);
		if (matcher.find()) {// 以“[*]”结尾的
			String group = matcher.group();
			parent = getParent(object, attribute.substring(0, attribute.length() - group.length()),
					group.substring(1, group.length() - 1), throwWhenAbsent);
		} else {
			int index = attribute.lastIndexOf(".");
			if (index > 0) {
				parent = getParent(object, attribute.substring(0, index), attribute.substring(index + 1),
						throwWhenAbsent);
			}
		}
		return parent;
	}

	private static Object getParent(Object object, String parentAttribute, String attribute, boolean throwWhenAbsent)
			throws Exception {
		if (StringUtils.isBlank(parentAttribute)) {
			return object;
		} else {
			Object parent = getValue(object, parentAttribute);
			if (parent == null) {
				parent = getParent(object, parentAttribute, throwWhenAbsent);
				setValueInner(object, attribute, parent, throwWhenAbsent);
			} else {
				Class<?> type = parent.getClass();
				String methodName = toMethodName(SET, attribute);
				List<Method> setMethods = getSetMethods(type).get(methodName);
				if (setMethods == null) {
					Field field = getFields(type).get(attribute);
					if (field == null) {
						if (throwWhenAbsent) {
							throw new IllegalArgumentException(StringUtils.concat("There is no set method named ",
									methodName, " or field named ", attribute, " in class ", type.getName()));
						}
					} else {
						Object grandparent = parent;
						parent = field.getType().getConstructor().newInstance();
						setValueInner(grandparent, attribute, parent, throwWhenAbsent);
					}
				} else {
					int count = setMethods.size();
					if (count == 1) {
						Object grandparent = parent;
						parent = setMethods.get(0).getParameterTypes()[0].getConstructor().newInstance();
						setValueInner(grandparent, attribute, parent, throwWhenAbsent);
					} else {
						throw new IllegalArgumentException(StringUtils.concat("Due to the feild ", attribute,
								" is null and there is ", count, " set methods named ", methodName, " for it in class ",
								type.getName(), ", we don't know which one to use for instantiation"));
					}
				}
			}
			return parent;
		}
	}

}
