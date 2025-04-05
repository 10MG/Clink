package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.types.DataType;

/**
 * 基础数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public abstract class BasicDataTypeFactory extends AbstractDataTypeFactory {

	protected static final String NOT_NULL_OR_NON_REGEX = "^[\\s]*([Nn][Oo][Tt][\\s]+[Nn][Uu][Ll]{2}){0,1}$";

	abstract DataType create();

	@Override
	public boolean supported(String desc) {
		return desc.trim().matches(NOT_NULL_OR_NON_REGEX);
	}

	@Override
	public DataType create(String desc) {
		DataType dataType = create();
		if (notNull(desc)) {
			return dataType.notNull();
		}
		return dataType;
	}
}
