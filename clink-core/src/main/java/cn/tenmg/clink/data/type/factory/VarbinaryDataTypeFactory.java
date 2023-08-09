package cn.tenmg.clink.data.type.factory;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.VarBinaryType;

/**
 * {@code VARBINARY} 数据类型工厂
 * 
 * @author June wjzhao@aliyun.com
 * 
 * @since 1.6.0
 */
public class VarbinaryDataTypeFactory extends ScalableDataTypeFactory {

	@Override
	DataType create(int scale) {
		return DataTypes.VARBINARY(scale);
	}

	@Override
	DataType create() {
		return DataTypes.VARBINARY(VarBinaryType.DEFAULT_LENGTH);
	}

}
