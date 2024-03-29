package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.types.DataType;

import cn.tenmg.clink.utils.SQLUtils;

/**
 * 可指定长度的数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public abstract class PrecisionDataTypeFactory extends BasicDataTypeFactory {

	private static final String SCALABLE_REGEX = "^[\\s]*(\\([\\s]*[0-9]+[\\s]*\\)[\\s]*){0,1}([Nn][Oo][Tt][\\s]+[Nn][Uu][Ll]{2}){0,1}$";

	/**
	 * 根据长度生成数据类型 {@code DataType} 对象
	 * 
	 * @param precision
	 *            长度
	 * @return 数据类型 {@code DataType} 对象
	 */
	abstract DataType create(int precision);

	@Override
	public boolean supported(String desc) {
		return desc.trim().matches(SCALABLE_REGEX);
	}

	@Override
	public DataType create(String desc) {
		if (desc.startsWith(SQLUtils.LEFT_BRACKET)) {
			int precisionEnd = desc.indexOf(SQLUtils.RIGTH_BRACKET), next = precisionEnd + 1;
			DataType dataType = create(Integer.parseInt(desc.substring(1, precisionEnd).trim()));
			if (next < desc.length() && notNull(desc.substring(next))) {
				return dataType.notNull();
			}
			return dataType;
		}
		return super.create(desc);
	}

}
