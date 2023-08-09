package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.CharType;

/**
 * {@code CHAR} 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class CharDataTypeFactory extends ScalableDataTypeFactory {

	@Override
	DataType create(int scale) {
		return DataTypes.CHAR(scale);
	}

	@Override
	DataType create() {
		return DataTypes.CHAR(CharType.DEFAULT_LENGTH);
	}

}
