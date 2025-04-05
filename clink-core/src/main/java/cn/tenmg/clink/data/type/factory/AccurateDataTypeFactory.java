package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.types.DataType;

import cn.tenmg.clink.utils.SQLUtils;

/**
 * 精确（可指定精度的）数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public abstract class AccurateDataTypeFactory extends AbstractDataTypeFactory {

	protected static final String ACCURATE_REGEX = "^(\\([\\s]*[0-9]+[\\s]*(,[\\s]*[0-9]+[\\s]*){0,1}\\)){0,1}[\\s]*([Nn][Oo][Tt][\\s]+[Nn][Uu][Ll]{2}){0,1}$";

	/**
	 * 根据长度、精度生成数据类型 {@code DataType} 对象
	 * 
	 * @param precision
	 *            长度
	 * @param scale
	 *            精度
	 * @return 数据类型 {@code DataType} 对象
	 */
	abstract DataType create(int precision, int scale);

	/**
	 * 根据长度生成默认精度的数据类型 {@code DataType} 对象
	 * 
	 * @param precision
	 *            长度
	 * @return 数据类型 {@code DataType} 对象
	 */
	abstract DataType create(int precision);

	/**
	 * 生成默认长度、精度的数据类型 {@code DataType} 对象
	 * 
	 * @return 数据类型 {@code DataType} 对象
	 */
	abstract DataType create();

	@Override
	public boolean supported(String desc) {
		return desc.trim().matches(ACCURATE_REGEX);
	}

	@Override
	public DataType create(String desc) {
		int precisionStart = desc.indexOf(SQLUtils.LEFT_BRACKET) + 1;
		if (precisionStart > 0) {
			int splitIndex = desc.indexOf(",");
			if (splitIndex > 0) {
				int scaleEnd = desc.indexOf(SQLUtils.RIGTH_BRACKET), next = scaleEnd + 1;
				DataType dataType = create(Integer.parseInt(desc.substring(precisionStart, splitIndex).trim()),
						Integer.parseInt(desc.substring(splitIndex + 1, scaleEnd)));
				if (next < desc.length() && notNull(desc.substring(next))) {
					return dataType.notNull();
				}
				return dataType;
			} else {
				int precisionEnd = desc.indexOf(SQLUtils.RIGTH_BRACKET), next = precisionEnd + 1;
				DataType dataType = create(Integer.parseInt(desc.substring(precisionStart, precisionEnd).trim()));
				if (next < desc.length() && notNull(desc.substring(next))) {
					return dataType.notNull();
				}
				return dataType;
			}
		} else {
			DataType dataType = create();
			if (notNull(desc)) {
				return dataType.notNull();
			}
			return dataType;
		}
	}

}
