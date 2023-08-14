package cn.tenmg.clink.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

import org.apache.flink.table.types.DataType;

import cn.tenmg.clink.data.type.DataTypeFactory;
import cn.tenmg.clink.exception.UnsupportedTypeException;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.MapUtils;
import cn.tenmg.dsl.utils.SetUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 数据类型工具类
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public abstract class DataTypeUtils {

	private static Map<String, DataTypeFactory> factories = MapUtils.newHashMap();

	private static Set<Character> typeNameEndChars = SetUtils.newHashSet('(', '<');

	static {
		DataTypeFactory factory;
		ServiceLoader<DataTypeFactory> loader = ServiceLoader.load(DataTypeFactory.class);
		for (Iterator<DataTypeFactory> it = loader.iterator(); it.hasNext();) {
			factory = it.next();
			factories.put(factory.typeName(), factory);
		}
	}

	/**
	 * 从 Flink SQL 类型获取数据类型 {@code DataType} 对象
	 * 
	 * @param type
	 *            Flink SQL 类型
	 * @return 数据类型 {@code DataType} 对象
	 */
	public static DataType fromFlinkSQLType(String type) {
		if (StringUtils.isBlank(type)) {
			throw new UnsupportedTypeException("Blank type not supported");
		}
		type = type.trim();
		String typeName = type, desc = "";
		int typeNameEndIndex = typeNameEndIndex(type);
		if (typeNameEndIndex > 0) {
			typeName = type.substring(0, typeNameEndIndex).trim();
			if (typeNameEndIndex < type.length()) {
				desc = type.substring(typeNameEndIndex);
			}
		}
		DataTypeFactory factory = factories.get(typeName);
		if (factory == null || !factory.supported(desc)) {
			throw new UnsupportedTypeException("Unsupported type: " + type);
		} else {
			DataType dataType = factory.create(desc);
			if (dataType == null) {
				throw new UnsupportedTypeException("Unsupported type: " + type);
			}
			return dataType;
		}
	}

	private static int typeNameEndIndex(String type) {
		char c;
		for (int i = 0, len = type.length(); i < len; i++) {
			c = type.charAt(i);
			if (c <= DSLUtils.BLANK_SPACE || typeNameEndChars.contains(c)) {
				return i;
			}
		}
		return -1;
	}

}
