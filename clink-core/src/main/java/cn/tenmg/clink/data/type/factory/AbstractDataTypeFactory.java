package cn.tenmg.clink.data.type.factory;

import cn.tenmg.clink.data.type.DataTypeFactory;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * 抽象数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public abstract class AbstractDataTypeFactory implements DataTypeFactory {

	protected static final String NOT_NULL_REGEX = "^[Nn][Oo][Tt][\\s]+[Nn][Uu][Ll]{2}$";

	private final String typeName = StringUtils
			.camelToUnderline(getClass().getSimpleName().replace("DataTypeFactory", "")).toUpperCase();

	@Override
	public String typeName() {
		return typeName;
	}

	protected static boolean notNull(String desc) {
		return desc.trim().matches(NOT_NULL_REGEX);
	}

}
