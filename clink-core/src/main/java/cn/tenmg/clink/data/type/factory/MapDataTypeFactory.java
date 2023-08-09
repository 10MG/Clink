package cn.tenmg.clink.data.type.factory;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import cn.tenmg.clink.exception.UnsupportedSymbolException;
import cn.tenmg.clink.utils.DataTypeUtils;

/**
 * {@code MAP} 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class MapDataTypeFactory extends AbstractDataTypeFactory {

	private static final String MAP_REGEX = "^(<[\\S]+,[\\S]+>)*[\\s]*([Nn][Oo][Tt][\\\\s]+[Nn][Uu][Ll]{2}){0,1}$";

	@Override
	public boolean supported(String desc) {
		return desc.trim().matches(MAP_REGEX);
	}

	@Override
	public DataType create(String desc) {
		char c;
		Character symbol;
		int deep = 0, keyStartIndex = 0, splitIndex = 0;
		Map<Integer, Character> symbols = new HashMap<Integer, Character>();
		StringBuilder fieldBuilder = new StringBuilder();
		for (int i = 0, len = desc.length(); i < len; i++) {
			c = desc.charAt(i);
			if (c == '<') {
				if (deep == 0) {
					keyStartIndex = i + 1;
				}
				symbols.put(++deep, c);
			} else if (c == '>') {
				symbol = symbols.get(deep--);
				if (symbol == null) {
					throw new UnsupportedSymbolException("Unexpected symbol '>'");
				} else if (symbol == '(') {
					throw new UnsupportedSymbolException("Unsupported symbol '>' after '('");
				} else if (deep == 0) {
					return create(desc, keyStartIndex, splitIndex, i);
				}
			} else if (c == '(') {
				if (deep == 0) {
					keyStartIndex = i + 1;
				}
				symbols.put(++deep, c);
			} else if (c == ')') {
				symbol = symbols.get(deep--);
				if (symbol == null) {
					throw new UnsupportedSymbolException("Unexpected symbol ')'");
				} else if (symbol == '<') {
					throw new UnsupportedSymbolException("Unsupported symbol ')' after '<'");
				} else if (deep == 0) {
					return create(desc, keyStartIndex, splitIndex, i);
				}
			} else if (c == ',' && deep == 1) {
				splitIndex = i;
			}
			fieldBuilder.append(c);
		}
		return null;
	}

	DataType create(String desc, int keyStartIndex, int splitIndex, int valueEndIndex) {
		int next = valueEndIndex + 1;
		DataType dataType = DataTypes.MAP(DataTypeUtils.fromFlinkSQLType(desc.substring(keyStartIndex, splitIndex)),
				DataTypeUtils.fromFlinkSQLType(desc.substring(splitIndex + 1, valueEndIndex)));
		if (next < desc.length() && notNull(desc.substring(next))) {
			return dataType.notNull();
		}
		return dataType;
	}

}
