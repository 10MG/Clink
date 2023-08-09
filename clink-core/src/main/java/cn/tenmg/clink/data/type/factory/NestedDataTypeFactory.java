package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.types.DataType;

import cn.tenmg.clink.exception.UnsupportedTypeException;
import cn.tenmg.clink.utils.DataTypeUtils;

/**
 * 嵌套数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public abstract class NestedDataTypeFactory<T> extends AbstractDataTypeFactory {

	private static final String NESTED_REGEX = "^<[\\S]+>[\\s]*([Nn][Oo][Tt][\\\\s]+[Nn][Uu][Ll]{2}){0,1}$",
			RIGHT_ANGLE_BRACKET = ">";

	/**
	 * 根据嵌套对象生成数据类型 {@code DataType} 对象
	 * 
	 * @param dataType
	 *            嵌套类型对象
	 * @return 数据类型 {@code DataType} 对象
	 */
	abstract DataType create(DataType dataType);

	@Override
	public boolean supported(String desc) {
		return desc.trim().matches(NESTED_REGEX);
	}

	@Override
	public DataType create(String desc) {
		int index = desc.lastIndexOf(RIGHT_ANGLE_BRACKET);
		String nested = desc.substring(1, index);
		try {
			int next = index + 1;
			DataType dataType = create(DataTypeUtils.fromFlinkSQLType(nested));
			if (next < desc.length() && notNull(desc.substring(next))) {
				return dataType.notNull();
			}
			return dataType;
		} catch (Exception e) {
			throw new UnsupportedTypeException("Unsupported type: " + nested, e);
		}
	}

}
