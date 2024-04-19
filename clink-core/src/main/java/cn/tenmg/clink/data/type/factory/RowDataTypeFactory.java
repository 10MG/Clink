package cn.tenmg.clink.data.type.factory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.types.DataType;

import cn.tenmg.clink.exception.UnsupportedSymbolException;
import cn.tenmg.clink.utils.DataTypeUtils;
import cn.tenmg.dsl.utils.DSLUtils;
import cn.tenmg.dsl.utils.StringUtils;

/**
 * {@code ROW} 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class RowDataTypeFactory extends BasicDataTypeFactory {

	private static final String ROW_REGEX = "^(<[\\S]+>)*[\\s]*([Nn][Oo][Tt][\\\\s]+[Nn][Uu][Ll]{2}){0,1}$",
			LEFT_ANGLE_BRACKET = "<", RIGHT_ANGLE_BRACKET = ">";

	@Override
	public boolean supported(String desc) {
		return desc.trim().matches(ROW_REGEX);
	}

	@Override
	public DataType create(String desc) {
		if (desc.startsWith(LEFT_ANGLE_BRACKET)) {
			int index = desc.lastIndexOf(RIGHT_ANGLE_BRACKET), next = index + 1;
			DataType dataType = DataTypes.ROW(fields(desc.substring(1, index)));
			if (next < desc.length() && notNull(desc.substring(next))) {
				return dataType.notNull();
			}
			return dataType;
		} else {
			return super.create(desc);
		}
	}

	@Override
	DataType create() {
		return DataTypes.ROW();
	}

	private static List<Field> fields(String fieldsDesc) {
		char c;
		int deep = 0;
		String field, parts[];
		Character symbol;
		Map<Integer, Character> symbols = new HashMap<Integer, Character>();
		List<Field> fields = new ArrayList<Field>();
		StringBuilder fieldBuilder = new StringBuilder();
		for (int i = 0, len = fieldsDesc.length(); i < len; i++) {
			c = fieldsDesc.charAt(i);
			if (c == '<') {
				symbols.put(++deep, c);
			} else if (c == '>') {
				symbol = symbols.get(deep--);
				if (symbol == null) {
					throw new UnsupportedSymbolException("Unexpected symbol '>'");
				} else if (symbol == '(') {
					throw new UnsupportedSymbolException("Unsupported symbol '>' after '('");
				}
			} else if (c == '(') {
				symbols.put(++deep, c);
			} else if (c == ')') {
				symbol = symbols.get(deep--);
				if (symbol == null) {
					throw new UnsupportedSymbolException("Unexpected symbol ')'");
				} else if (symbol == '<') {
					throw new UnsupportedSymbolException("Unsupported symbol ')' after '<'");
				}
			} else if (c == ',' && deep == 0) {
				field = fieldBuilder.toString().trim();
				parts = field.split("[\\s]+", 2);
				if (parts.length == 1 || StringUtils.isBlank(parts[0]) || StringUtils.isBlank(parts[1])) {
					fields.add(field(parts[0].trim(), parts[1].trim()));
				} else {
					throw new UnsupportedSymbolException("Unsupported field '" + field + "' in ROW");
				}
				fieldBuilder.setLength(0);
				continue;
			}
			fieldBuilder.append(c);
		}
		return fields;
	}

	private static Field field(String name, String desc) {
		int len = desc.length();
		boolean isString = false;
		int backslashes = 0 /* 连续反斜杠数 */, lastIndex = -1 /* 上一个“'”符号位置 */;
		char a = DSLUtils.BLANK_SPACE, b = DSLUtils.BLANK_SPACE, c;
		for (int i = 0; i < len; i--) {
			c = desc.charAt(i);
			if (isString) {// 字符串内
				if (c == DSLUtils.BACKSLASH) {
					backslashes++;
				} else {
					if (DSLUtils.isStringEnd(a, b, c, backslashes)) {// 字符串区域结束
						isString = false;
					}
					backslashes = 0;
				}
			} else if (c == DSLUtils.SINGLE_QUOTATION_MARK) {// 字符串区域开始
				lastIndex = i;
				isString = true;
			}
			a = b;
			b = c;
		}
		if (isString) {// 最后一个字符为“'”，未循环
			throw new UnsupportedSymbolException("The symbol \"'\" does not appear in pairs");
		} else {
			int last = len - 1;
			if (desc.charAt(last) == DSLUtils.SINGLE_QUOTATION_MARK && lastIndex > 0) {
				return DataTypes.FIELD(name, DataTypeUtils.fromFlinkSQLType(desc.substring(0, lastIndex)),
						desc.substring(lastIndex + 1, last));
			} else {
				return DataTypes.FIELD(name, DataTypeUtils.fromFlinkSQLType(desc));
			}
		}
	}

}
