package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Resolution;
import org.apache.flink.table.types.DataType;

import cn.tenmg.clink.exception.UnsupportedTypeException;
import cn.tenmg.clink.utils.SQLUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * {@code INTERVAL} 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class IntervalDataTypeFactory extends AbstractDataTypeFactory {

	private static final String DAY_REGEX = regex("(", "DAY", "[\\s]*(\\([\\s]*[0-9]+[\\s]*\\))*)"),
			HOUR_REGEX = regex("(", "HOUR", ")"), MINUTE_REGEX = regex("(", "MINUTE", ")"),
			MONTH_REGEX = regex("(", "MONTH", ")"),
			SECOND_REGEX = regex("(", "SECOND", "[\\s]*(\\([\\s]*[0-9]+[\\s]*\\))*)"),
			YEAR_REGEX = regex("(", "YEAR", "[\\s]*(\\([\\s]*[0-9]+[\\s]*\\))*)"),
			INTERVAL_REGEX = StringUtils.concat("^(", DAY_REGEX, '|', HOUR_REGEX, '|', MINUTE_REGEX, '|', MONTH_REGEX,
					'|', SECOND_REGEX, '|', YEAR_REGEX, ")([\\s]+([Tt][Oo])[\\s]+(", DAY_REGEX, '|', HOUR_REGEX, '|',
					MINUTE_REGEX, '|', MONTH_REGEX, '|', SECOND_REGEX, '|', YEAR_REGEX, ")){0,1}$");

	@Override
	public boolean supported(String desc) {
		return desc.trim().matches(INTERVAL_REGEX);
	}

	@Override
	public DataType create(String desc) {
		String[] parts = desc.split("[Tt][Oo]", 2);
		if (parts.length > 1) {
			return DataTypes.INTERVAL(resolution(parts[0].trim()), resolution(parts[1].trim()));
		} else {
			int index = desc.indexOf(SQLUtils.RIGTH_BRACKET), next = index + 1;
			if (index > 0 && next < desc.length()) {
				DataType dataType = DataTypes.INTERVAL(resolution(desc.substring(0, index).trim()));
				if (notNull(desc.substring(next))) {
					return dataType.notNull();
				}
			}
			return DataTypes.INTERVAL(resolution(desc.trim()));
		}
	}

	private static String regex(String prefix, String typeName, String suffix) {
		char c;
		StringBuilder regex = new StringBuilder(prefix);
		for (int i = 0, len = typeName.length(); i < len; i++) {
			c = typeName.charAt(i);
			regex.append('[').append(Character.toUpperCase(c)).append(Character.toLowerCase(c)).append(']');
		}
		regex.append(suffix);
		return regex.toString();
	}

	private Resolution resolution(String resolution) {
		if (resolution.matches(DAY_REGEX)) {
			int start = resolution.indexOf(SQLUtils.LEFT_BRACKET);
			if (start > 0) {
				return DataTypes.DAY(precision(resolution, start));
			}
			return DataTypes.DAY();
		} else if (resolution.matches(HOUR_REGEX)) {
			return DataTypes.HOUR();
		} else if (resolution.matches(MINUTE_REGEX)) {
			return DataTypes.MINUTE();
		} else if (resolution.matches(MONTH_REGEX)) {
			return DataTypes.MONTH();
		} else if (resolution.matches(SECOND_REGEX)) {
			int start = resolution.indexOf(SQLUtils.LEFT_BRACKET);
			if (start > 0) {
				return DataTypes.SECOND(precision(resolution, start));
			}
			return DataTypes.SECOND();
		} else if (resolution.matches(YEAR_REGEX)) {
			int start = resolution.indexOf(SQLUtils.LEFT_BRACKET);
			if (start > 0) {
				return DataTypes.YEAR(precision(resolution, start));
			}
			return DataTypes.YEAR();
		}
		throw new UnsupportedTypeException("Unsupported INTERVAL type: " + resolution);
	}

	private static int precision(String resolution, int start) {
		return Integer.parseInt(resolution.substring(start + 1, resolution.length() - 1).trim());
	}

}
