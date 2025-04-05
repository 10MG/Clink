package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BinaryType;

/**
 * {@code BINARY} 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class BinaryDataTypeFactory extends PrecisionDataTypeFactory {

	@Override
	DataType create(int precision) {
		return DataTypes.BINARY(precision);
	}

	@Override
	DataType create() {
		return DataTypes.BINARY(BinaryType.DEFAULT_LENGTH);
	}
}
